"""
Payment Service - Fault Tolerance: CIRCUIT BREAKER
Handles payment processing. Circuit opens when failure rate exceeds threshold.
States: CLOSED (normal) → OPEN (rejecting) → HALF-OPEN (testing recovery)
"""

import os
import time
import random
import logging
import threading
from flask import Flask, request, jsonify
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

logging.basicConfig(level=logging.INFO, format='%(asctime)s [PAYMENT] %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ─── OpenTelemetry ────────────────────────────────────────────────────────────
JAEGER_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4318/v1/traces")
provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=JAEGER_ENDPOINT)))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("payment-service")

# ─── Prometheus Metrics ───────────────────────────────────────────────────────
PAYMENT_REQUESTS = Counter("payment_requests_total", "Total payment requests", ["status"])
PAYMENT_LATENCY  = Histogram("payment_latency_seconds", "Payment latency")
CB_STATE_GAUGE   = Gauge("circuit_breaker_state", "Circuit breaker state (0=closed,1=open,2=half_open)")
CB_TRIPS         = Counter("circuit_breaker_trips_total", "Times circuit breaker opened")

# ─── Circuit Breaker Implementation ──────────────────────────────────────────
class CircuitBreaker:
    """
    Simple Circuit Breaker.
    Config is env-driven so you can change threshold without rebuild.
    """
    CLOSED    = "CLOSED"
    OPEN      = "OPEN"
    HALF_OPEN = "HALF_OPEN"

    def __init__(self):
        self.state          = self.CLOSED
        self.failure_count  = 0
        self.success_count  = 0
        self.last_open_time = None
        self.lock           = threading.Lock()

        # Config (can be overridden per test config)
        self.failure_threshold   = int(float(os.getenv("CB_FAILURE_THRESHOLD", "3")))   # failures before open
        self.recovery_timeout    = float(os.getenv("CB_RECOVERY_TIMEOUT",    "10.0"))   # seconds open before half-open
        self.success_threshold   = int(float(os.getenv("CB_SUCCESS_THRESHOLD","2")))    # successes in half-open to close

    def call(self, fn):
        with self.lock:
            if self.state == self.OPEN:
                elapsed = time.time() - self.last_open_time
                if elapsed >= self.recovery_timeout:
                    logger.info("[CB] Transitioning OPEN → HALF_OPEN")
                    self.state = self.HALF_OPEN
                    self.success_count = 0
                    CB_STATE_GAUGE.set(2)
                else:
                    logger.warning(f"[CB] OPEN — rejecting request ({elapsed:.1f}s / {self.recovery_timeout}s)")
                    raise CircuitOpenError("Circuit breaker is OPEN")

        try:
            result = fn()
            with self.lock:
                if self.state == self.HALF_OPEN:
                    self.success_count += 1
                    if self.success_count >= self.success_threshold:
                        logger.info("[CB] HALF_OPEN → CLOSED (recovered)")
                        self.state = self.CLOSED
                        self.failure_count = 0
                        CB_STATE_GAUGE.set(0)
                elif self.state == self.CLOSED:
                    self.failure_count = max(0, self.failure_count - 1)  # decay on success
            return result
        except CircuitOpenError:
            raise
        except Exception as e:
            with self.lock:
                self.failure_count += 1
                logger.warning(f"[CB] Failure recorded ({self.failure_count}/{self.failure_threshold})")
                if self.state in (self.CLOSED, self.HALF_OPEN) and self.failure_count >= self.failure_threshold:
                    logger.error("[CB] Threshold reached → OPEN")
                    self.state = self.OPEN
                    self.last_open_time = time.time()
                    CB_STATE_GAUGE.set(1)
                    CB_TRIPS.inc()
            raise

    def status(self):
        return {
            "state": self.state,
            "failure_count": self.failure_count,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout_s": self.recovery_timeout,
        }


class CircuitOpenError(Exception):
    pass


cb = CircuitBreaker()

# ─── Flask App ────────────────────────────────────────────────────────────────
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)

# ─── Fault Injection State ────────────────────────────────────────────────────
fault_state = {
    "inject_crash":   False,
    "inject_latency": False,
    "latency_ms":     0,
    "fail_rate":      0.0   # 0.0–1.0: probability of random failure
}


def process_payment_logic(order_id, amount):
    """Simulates actual payment processing. Can be made to fail via fault injection."""
    if fault_state["inject_crash"]:
        raise Exception("Payment service crash injected")

    if fault_state["inject_latency"]:
        time.sleep(fault_state["latency_ms"] / 1000)

    if random.random() < fault_state["fail_rate"]:
        raise Exception(f"Random payment failure (rate={fault_state['fail_rate']})")

    # Simulate processing time
    time.sleep(random.uniform(0.05, 0.15))
    return {"transaction_id": f"TXN-{order_id}", "amount": amount, "status": "approved"}


# ─── Routes ───────────────────────────────────────────────────────────────────
@app.route("/payment/process", methods=["POST"])
def process_payment():
    start = time.time()
    data     = request.json or {}
    order_id = data.get("order_id", "unknown")
    amount   = data.get("amount", 0)

    with tracer.start_as_current_span("payment.process") as span:
        span.set_attribute("order.id", order_id)
        span.set_attribute("payment.amount", amount)
        span.set_attribute("circuit_breaker.state", cb.state)

        try:
            result = cb.call(lambda: process_payment_logic(order_id, amount))
            duration = time.time() - start
            PAYMENT_LATENCY.observe(duration)
            PAYMENT_REQUESTS.labels(status="success").inc()
            logger.info(f"[PAYMENT] Order {order_id} approved in {duration:.3f}s")
            return jsonify(result)

        except CircuitOpenError:
            PAYMENT_REQUESTS.labels(status="circuit_open").inc()
            return jsonify({"error": "Payment service unavailable (circuit open)", "circuit_state": cb.state}), 503

        except Exception as e:
            PAYMENT_REQUESTS.labels(status="failed").inc()
            return jsonify({"error": str(e)}), 500


@app.route("/payment/status", methods=["GET"])
def payment_status():
    return jsonify({"service": "payment", "status": "up", "circuit_breaker": cb.status()})


@app.route("/payment/health", methods=["GET"])
def health():
    state_map = {cb.CLOSED: "up", cb.HALF_OPEN: "degraded", cb.OPEN: "down"}
    return jsonify({"service": "payment", "status": state_map[cb.state], "circuit_breaker": cb.status()})


# ─── Fault Injection Admin Endpoints ─────────────────────────────────────────
@app.route("/fault/crash", methods=["POST"])
def inject_crash():
    body = request.json or {}
    fault_state["inject_crash"] = body.get("enabled", False)
    logger.warning(f"[FAULT] Crash: {fault_state['inject_crash']}")
    return jsonify({"fault": "crash", "enabled": fault_state["inject_crash"]})


@app.route("/fault/latency", methods=["POST"])
def inject_latency():
    body = request.json or {}
    fault_state["inject_latency"] = body.get("enabled", False)
    fault_state["latency_ms"]     = body.get("latency_ms", 3000)
    logger.warning(f"[FAULT] Latency: {fault_state['latency_ms']}ms")
    return jsonify({"fault": "latency", "state": fault_state})


@app.route("/fault/fail_rate", methods=["POST"])
def inject_fail_rate():
    body = request.json or {}
    fault_state["fail_rate"] = float(body.get("rate", 0.5))
    logger.warning(f"[FAULT] Fail rate: {fault_state['fail_rate']}")
    return jsonify({"fault": "fail_rate", "rate": fault_state["fail_rate"]})


@app.route("/fault/reset", methods=["POST"])
def reset_faults():
    fault_state.update({"inject_crash": False, "inject_latency": False, "latency_ms": 0, "fail_rate": 0.0})
    logger.info("[FAULT] Reset all faults")
    return jsonify({"fault": "reset"})


@app.route("/fault/cb/reset", methods=["POST"])
def reset_circuit_breaker():
    with cb.lock:
        cb.state = cb.CLOSED
        cb.failure_count = 0
        CB_STATE_GAUGE.set(0)
    return jsonify({"circuit_breaker": "manually reset", "state": cb.state})


@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)
