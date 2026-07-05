"""
Order Service - Fault Tolerance: TIMEOUT MECHANISM
Entry point for client requests. Coordinates calls to Payment, Inventory, Notification.
"""

import os
import time
import uuid
import logging
import requests
from flask import Flask, request, jsonify
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s [ORDER] %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ─── OpenTelemetry Setup ─────────────────────────────────────────────────────
JAEGER_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4318/v1/traces")
provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=JAEGER_ENDPOINT)))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("order-service")

# ─── Prometheus Metrics ───────────────────────────────────────────────────────
ORDER_REQUESTS  = Counter("order_requests_total", "Total order requests", ["status"])
ORDER_LATENCY   = Histogram("order_latency_seconds", "Order request latency")
TIMEOUT_COUNTER = Counter("order_timeout_total", "Total timeouts triggered", ["target_service"])

# ─── Flask App ────────────────────────────────────────────────────────────────
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)
RequestsInstrumentor().instrument()

# ─── Config (env-driven so you can change without rebuilding) ─────────────────
PAYMENT_URL     = os.getenv("PAYMENT_URL",     "http://payment-service:5001")
INVENTORY_URL   = os.getenv("INVENTORY_URL",   "http://inventory-service:5002")
NOTIFICATION_URL= os.getenv("NOTIFICATION_URL","http://notification-service:5003")

# TIMEOUT CONFIG — change these to switch between Static and Dynamic configs
PAYMENT_TIMEOUT   = float(os.getenv("PAYMENT_TIMEOUT",    "3.0"))   # seconds
INVENTORY_TIMEOUT = float(os.getenv("INVENTORY_TIMEOUT",  "3.0"))
NOTIFY_TIMEOUT    = float(os.getenv("NOTIFY_TIMEOUT",     "2.0"))

# ─── Fault Injection State ────────────────────────────────────────────────────
fault_state = {
    "inject_latency": False,
    "latency_ms": 0,
    "inject_crash": False
}

# ─── Helper: call with timeout ────────────────────────────────────────────────
def call_service(service_name, url, method="post", payload=None, timeout=3.0):
    """Calls a downstream service with timeout. Returns (response_json, error_msg)."""
    try:
        fn = requests.post if method == "post" else requests.get
        resp = fn(url, json=payload, timeout=timeout)
        resp.raise_for_status()
        return resp.json(), None
    except requests.exceptions.Timeout:
        TIMEOUT_COUNTER.labels(target_service=service_name).inc()
        logger.warning(f"[TIMEOUT] {service_name} did not respond within {timeout}s")
        return None, f"timeout calling {service_name}"
    except requests.exceptions.ConnectionError:
        logger.error(f"[CONNECTION ERROR] Cannot reach {service_name}")
        return None, f"connection error to {service_name}"
    except Exception as e:
        logger.error(f"[ERROR] {service_name}: {e}")
        return None, str(e)

# ─── Routes ───────────────────────────────────────────────────────────────────
@app.route("/order", methods=["POST"])
def create_order():
    start = time.time()
    order_id = str(uuid.uuid4())[:8]

    # Fault injection: simulate crash
    if fault_state["inject_crash"]:
        ORDER_REQUESTS.labels(status="crash").inc()
        return jsonify({"error": "Order service crash injected"}), 500

    # Fault injection: simulate latency
    if fault_state["inject_latency"]:
        time.sleep(fault_state["latency_ms"] / 1000)

    data = request.json or {}
    item_id  = data.get("item_id", "ITEM-001")
    quantity = data.get("quantity", 1)
    amount   = data.get("amount", 100.0)

    logger.info(f"[ORDER {order_id}] Received — item={item_id} qty={quantity} amount={amount}")

    with tracer.start_as_current_span("order.process") as span:
        span.set_attribute("order.id", order_id)

        # Step 1: Check inventory
        inv_data, inv_err = call_service(
            "inventory-service",
            f"{INVENTORY_URL}/inventory/check",
            method="post",
            payload={"item_id": item_id, "quantity": quantity},
            timeout=INVENTORY_TIMEOUT
        )
        if inv_err:
            ORDER_REQUESTS.labels(status="failed_inventory").inc()
            return jsonify({"order_id": order_id, "status": "failed", "reason": inv_err}), 503

        if not inv_data.get("available"):
            ORDER_REQUESTS.labels(status="out_of_stock").inc()
            return jsonify({"order_id": order_id, "status": "failed", "reason": "out of stock"}), 400

        # Step 2: Process payment
        pay_data, pay_err = call_service(
            "payment-service",
            f"{PAYMENT_URL}/payment/process",
            method="post",
            payload={"order_id": order_id, "amount": amount},
            timeout=PAYMENT_TIMEOUT
        )
        if pay_err:
            ORDER_REQUESTS.labels(status="failed_payment").inc()
            return jsonify({"order_id": order_id, "status": "failed", "reason": pay_err}), 503

        # Step 3: Reserve inventory
        call_service(
            "inventory-service",
            f"{INVENTORY_URL}/inventory/reserve",
            method="post",
            payload={"item_id": item_id, "quantity": quantity, "order_id": order_id},
            timeout=INVENTORY_TIMEOUT
        )

        # Step 4: Send notification (non-critical — don't fail order if this times out)
        call_service(
            "notification-service",
            f"{NOTIFICATION_URL}/notify",
            method="post",
            payload={"order_id": order_id, "item_id": item_id, "amount": amount},
            timeout=NOTIFY_TIMEOUT
        )

    duration = time.time() - start
    ORDER_LATENCY.observe(duration)
    ORDER_REQUESTS.labels(status="success").inc()
    logger.info(f"[ORDER {order_id}] Completed in {duration:.3f}s")
    return jsonify({"order_id": order_id, "status": "success", "duration_ms": round(duration * 1000, 2)})


@app.route("/order/health", methods=["GET"])
def health():
    return jsonify({"service": "order", "status": "up"})


# ─── Fault Injection Admin Endpoints ─────────────────────────────────────────
@app.route("/fault/latency", methods=["POST"])
def inject_latency():
    body = request.json or {}
    fault_state["inject_latency"] = body.get("enabled", False)
    fault_state["latency_ms"] = body.get("latency_ms", 2000)
    logger.warning(f"[FAULT] Latency injection: {fault_state}")
    return jsonify({"fault": "latency", "state": fault_state})


@app.route("/fault/crash", methods=["POST"])
def inject_crash():
    body = request.json or {}
    fault_state["inject_crash"] = body.get("enabled", False)
    logger.warning(f"[FAULT] Crash injection: {fault_state['inject_crash']}")
    return jsonify({"fault": "crash", "enabled": fault_state["inject_crash"]})


@app.route("/fault/reset", methods=["POST"])
def reset_faults():
    fault_state["inject_latency"] = False
    fault_state["inject_crash"] = False
    fault_state["latency_ms"] = 0
    logger.info("[FAULT] All faults reset")
    return jsonify({"fault": "reset", "state": fault_state})


@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
