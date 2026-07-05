"""
Notification Service - Fault Tolerance: RETRY WITH EXPONENTIAL BACKOFF
Sends order confirmation after successful order. Retries on transient failures
with exponential backoff to avoid retry storms.
"""

import os
import time
import random
import logging
from flask import Flask, request, jsonify
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

logging.basicConfig(level=logging.INFO, format='%(asctime)s [NOTIFY] %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ─── OpenTelemetry ────────────────────────────────────────────────────────────
JAEGER_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4318/v1/traces")
provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=JAEGER_ENDPOINT)))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("notification-service")

# ─── Prometheus Metrics ───────────────────────────────────────────────────────
NOTIFY_REQUESTS = Counter("notification_requests_total", "Total notification requests", ["status"])
NOTIFY_LATENCY  = Histogram("notification_latency_seconds", "Notification latency")
RETRY_COUNTER   = Counter("notification_retries_total", "Total retry attempts")
RETRY_EXHAUSTED = Counter("notification_retry_exhausted_total", "Retries exhausted (gave up)")

# ─── Retry Config (env-driven) ────────────────────────────────────────────────
MAX_RETRIES    = int(float(os.getenv("RETRY_MAX",         "3")))
BASE_DELAY     = float(os.getenv("RETRY_BASE_DELAY",      "0.5"))   # seconds
BACKOFF_FACTOR = float(os.getenv("RETRY_BACKOFF_FACTOR",  "2.0"))   # exponential multiplier
MAX_DELAY      = float(os.getenv("RETRY_MAX_DELAY",       "10.0"))  # cap on retry delay
JITTER         = os.getenv("RETRY_JITTER", "true").lower() == "true"

# ─── Flask App ────────────────────────────────────────────────────────────────
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)

# ─── Fault Injection State ────────────────────────────────────────────────────
fault_state = {
    "inject_crash":    False,
    "inject_latency":  False,
    "latency_ms":      0,
    "transient_rate":  0.0   # probability of transient failure (should retry)
}

# ─── Retry Helper ────────────────────────────────────────────────────────────
def with_retry(fn, max_retries=MAX_RETRIES, base_delay=BASE_DELAY,
               backoff_factor=BACKOFF_FACTOR, max_delay=MAX_DELAY):
    """
    Execute fn() with exponential backoff retry.
    Returns (result, attempts, total_delay)
    """
    attempt    = 0
    total_wait = 0.0

    while attempt <= max_retries:
        try:
            result = fn()
            return result, attempt, total_wait
        except TransientError as e:
            attempt += 1
            RETRY_COUNTER.inc()
            if attempt > max_retries:
                RETRY_EXHAUSTED.inc()
                logger.error(f"[RETRY] Exhausted after {max_retries} retries. Last error: {e}")
                raise MaxRetriesExceeded(f"Failed after {max_retries} retries: {e}")

            delay = min(base_delay * (backoff_factor ** (attempt - 1)), max_delay)
            if JITTER:
                delay = delay * (0.5 + random.random() * 0.5)  # ±50% jitter

            total_wait += delay
            logger.warning(f"[RETRY] Attempt {attempt}/{max_retries} failed. "
                           f"Retrying in {delay:.2f}s — {e}")
            time.sleep(delay)
        except Exception as e:
            # Non-transient error: do NOT retry
            logger.error(f"[RETRY] Non-transient error, no retry: {e}")
            raise


class TransientError(Exception):
    """Errors that should trigger a retry (network glitches, temporary failures)."""
    pass


class MaxRetriesExceeded(Exception):
    pass


def send_notification_logic(order_id, item_id, amount):
    """
    Simulates sending a notification (email/SMS/push).
    Raises TransientError for retryable failures.
    """
    if fault_state["inject_crash"]:
        raise Exception("Notification service crash injected (non-transient)")

    if fault_state["inject_latency"]:
        time.sleep(fault_state["latency_ms"] / 1000)

    if random.random() < fault_state["transient_rate"]:
        raise TransientError(f"Transient send failure (rate={fault_state['transient_rate']})")

    # Simulate normal send time
    time.sleep(random.uniform(0.03, 0.10))

    logger.info(f"[NOTIFY] ✓ Notification sent — order={order_id} item={item_id} amount={amount}")
    return {
        "notification_id": f"NOTIF-{order_id}",
        "channel": "email",
        "status": "sent",
        "order_id": order_id,
    }


# ─── Routes ───────────────────────────────────────────────────────────────────
@app.route("/notify", methods=["POST"])
def notify():
    start = time.time()
    data     = request.json or {}
    order_id = data.get("order_id", "unknown")
    item_id  = data.get("item_id",  "unknown")
    amount   = data.get("amount",   0)

    with tracer.start_as_current_span("notification.send") as span:
        span.set_attribute("order.id", order_id)
        span.set_attribute("retry.max", MAX_RETRIES)

        try:
            result, attempts, wait = with_retry(
                lambda: send_notification_logic(order_id, item_id, amount)
            )
            duration = time.time() - start
            NOTIFY_LATENCY.observe(duration)
            NOTIFY_REQUESTS.labels(status="success").inc()
            span.set_attribute("retry.attempts", attempts)

            return jsonify({
                **result,
                "retry_attempts": attempts,
                "total_wait_s": round(wait, 3),
                "duration_ms": round(duration * 1000, 2)
            })

        except MaxRetriesExceeded as e:
            NOTIFY_REQUESTS.labels(status="retry_exhausted").inc()
            return jsonify({"error": str(e), "order_id": order_id, "notification": "failed"}), 503

        except Exception as e:
            NOTIFY_REQUESTS.labels(status="failed").inc()
            return jsonify({"error": str(e)}), 500


@app.route("/notify/health", methods=["GET"])
def health():
    return jsonify({
        "service": "notification",
        "status": "up",
        "retry_config": {
            "max_retries": MAX_RETRIES,
            "base_delay_s": BASE_DELAY,
            "backoff_factor": BACKOFF_FACTOR,
            "max_delay_s": MAX_DELAY,
            "jitter": JITTER
        }
    })


# ─── Fault Injection Admin Endpoints ─────────────────────────────────────────
@app.route("/fault/latency", methods=["POST"])
def inject_latency():
    body = request.json or {}
    fault_state["inject_latency"] = body.get("enabled", False)
    fault_state["latency_ms"]     = body.get("latency_ms", 2000)
    return jsonify({"fault": "latency", "state": fault_state})


@app.route("/fault/crash", methods=["POST"])
def inject_crash():
    body = request.json or {}
    fault_state["inject_crash"] = body.get("enabled", False)
    return jsonify({"fault": "crash", "enabled": fault_state["inject_crash"]})


@app.route("/fault/transient", methods=["POST"])
def inject_transient():
    """Set transient failure rate 0.0–1.0 to test retry mechanism."""
    body = request.json or {}
    fault_state["transient_rate"] = float(body.get("rate", 0.7))
    logger.warning(f"[FAULT] Transient rate: {fault_state['transient_rate']}")
    return jsonify({"fault": "transient", "rate": fault_state["transient_rate"]})


@app.route("/fault/reset", methods=["POST"])
def reset_faults():
    fault_state.update({"inject_crash": False, "inject_latency": False, "latency_ms": 0, "transient_rate": 0.0})
    return jsonify({"fault": "reset"})


@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5003, debug=False)
