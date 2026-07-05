"""
Inventory Service - Fault Tolerance: BULKHEAD ISOLATION
Manages stock check and reservation. Separate thread pools for check vs. reserve
operations prevent one pool from exhausting shared resources.
"""

import os
import time
import random
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from flask import Flask, request, jsonify
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

logging.basicConfig(level=logging.INFO, format='%(asctime)s [INVENTORY] %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ─── OpenTelemetry ────────────────────────────────────────────────────────────
JAEGER_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4318/v1/traces")
provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=JAEGER_ENDPOINT)))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("inventory-service")

# ─── Prometheus Metrics ───────────────────────────────────────────────────────
INV_REQUESTS    = Counter("inventory_requests_total", "Total inventory requests", ["operation", "status"])
INV_LATENCY     = Histogram("inventory_latency_seconds", "Inventory latency", ["operation"])
BULKHEAD_REJECT = Counter("bulkhead_rejected_total", "Bulkhead rejected requests", ["pool"])
POOL_ACTIVE     = Gauge("bulkhead_active_threads", "Active threads per pool", ["pool"])

# ─── Bulkhead: Separate Thread Pools ─────────────────────────────────────────
CHECK_POOL_SIZE   = int(os.getenv("BULKHEAD_CHECK_POOL",   "5"))
RESERVE_POOL_SIZE = int(os.getenv("BULKHEAD_RESERVE_POOL", "3"))
POOL_TIMEOUT      = float(os.getenv("BULKHEAD_TIMEOUT",    "4.0"))

check_pool   = ThreadPoolExecutor(max_workers=CHECK_POOL_SIZE,   thread_name_prefix="check")
reserve_pool = ThreadPoolExecutor(max_workers=RESERVE_POOL_SIZE, thread_name_prefix="reserve")

# Track active threads per pool
check_active   = 0
reserve_active = 0
pool_lock      = threading.Lock()

# ─── Mock Inventory Store ─────────────────────────────────────────────────────
inventory_db = {
    "ITEM-001": 100,
    "ITEM-002": 50,
    "ITEM-003": 0,   # out of stock
    "ITEM-004": 25,
}
inventory_lock = threading.Lock()

# ─── Flask App ────────────────────────────────────────────────────────────────
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)

# ─── Fault Injection State ────────────────────────────────────────────────────
fault_state = {
    "inject_latency": False,
    "latency_ms":     0,
    "inject_crash":   False,
    "exhaust_pool":   False  # simulate pool saturation
}


def _do_check(item_id, quantity):
    """Actual check logic — runs inside bulkhead thread pool."""
    global check_active
    with pool_lock:
        check_active += 1
        POOL_ACTIVE.labels(pool="check").set(check_active)

    try:
        if fault_state["inject_crash"]:
            raise Exception("Inventory crash injected")
        if fault_state["inject_latency"]:
            time.sleep(fault_state["latency_ms"] / 1000)
        if fault_state["exhaust_pool"]:
            time.sleep(30)  # hold thread to simulate exhaustion

        time.sleep(random.uniform(0.02, 0.08))  # normal processing

        with inventory_lock:
            stock = inventory_db.get(item_id, 0)

        available = stock >= quantity
        logger.info(f"[CHECK] item={item_id} stock={stock} requested={quantity} available={available}")
        return {"item_id": item_id, "available": available, "stock": stock}
    finally:
        with pool_lock:
            check_active -= 1
            POOL_ACTIVE.labels(pool="check").set(check_active)


def _do_reserve(item_id, quantity, order_id):
    """Actual reserve logic — runs inside separate bulkhead thread pool."""
    global reserve_active
    with pool_lock:
        reserve_active += 1
        POOL_ACTIVE.labels(pool="reserve").set(reserve_active)

    try:
        if fault_state["inject_latency"]:
            time.sleep(fault_state["latency_ms"] / 1000)

        time.sleep(random.uniform(0.05, 0.12))  # normal processing

        with inventory_lock:
            stock = inventory_db.get(item_id, 0)
            if stock >= quantity:
                inventory_db[item_id] = stock - quantity
                success = True
            else:
                success = False

        logger.info(f"[RESERVE] order={order_id} item={item_id} qty={quantity} success={success}")
        return {"item_id": item_id, "reserved": success, "order_id": order_id}
    finally:
        with pool_lock:
            reserve_active -= 1
            POOL_ACTIVE.labels(pool="reserve").set(reserve_active)


# ─── Routes ───────────────────────────────────────────────────────────────────
@app.route("/inventory/check", methods=["POST"])
def check_inventory():
    start = time.time()
    data     = request.json or {}
    item_id  = data.get("item_id", "ITEM-001")
    quantity = int(data.get("quantity", 1))

    with tracer.start_as_current_span("inventory.check") as span:
        span.set_attribute("item.id", item_id)
        span.set_attribute("quantity", quantity)

        # Submit to CHECK bulkhead pool
        try:
            future = check_pool.submit(_do_check, item_id, quantity)
            result = future.result(timeout=POOL_TIMEOUT)
            INV_LATENCY.labels(operation="check").observe(time.time() - start)
            INV_REQUESTS.labels(operation="check", status="success").inc()
            return jsonify(result)

        except FutureTimeout:
            BULKHEAD_REJECT.labels(pool="check").inc()
            INV_REQUESTS.labels(operation="check", status="bulkhead_timeout").inc()
            logger.warning("[BULKHEAD] Check pool timed out — bulkhead protected system")
            return jsonify({"error": "Inventory check timed out (bulkhead)", "available": False}), 503

        except Exception as e:
            INV_REQUESTS.labels(operation="check", status="failed").inc()
            return jsonify({"error": str(e), "available": False}), 500


@app.route("/inventory/reserve", methods=["POST"])
def reserve_inventory():
    start = time.time()
    data     = request.json or {}
    item_id  = data.get("item_id", "ITEM-001")
    quantity = int(data.get("quantity", 1))
    order_id = data.get("order_id", "unknown")

    with tracer.start_as_current_span("inventory.reserve") as span:
        span.set_attribute("item.id", item_id)
        span.set_attribute("order.id", order_id)

        try:
            future = reserve_pool.submit(_do_reserve, item_id, quantity, order_id)
            result = future.result(timeout=POOL_TIMEOUT)
            INV_LATENCY.labels(operation="reserve").observe(time.time() - start)
            INV_REQUESTS.labels(operation="reserve", status="success").inc()
            return jsonify(result)

        except FutureTimeout:
            BULKHEAD_REJECT.labels(pool="reserve").inc()
            INV_REQUESTS.labels(operation="reserve", status="bulkhead_timeout").inc()
            logger.warning("[BULKHEAD] Reserve pool timed out")
            return jsonify({"error": "Reserve timed out (bulkhead)"}), 503

        except Exception as e:
            INV_REQUESTS.labels(operation="reserve", status="failed").inc()
            return jsonify({"error": str(e)}), 500


@app.route("/inventory/stock", methods=["GET"])
def get_stock():
    with inventory_lock:
        return jsonify({"inventory": dict(inventory_db)})


@app.route("/inventory/stock/reset", methods=["POST"])
def reset_stock():
    with inventory_lock:
        inventory_db.update({"ITEM-001": 100, "ITEM-002": 50, "ITEM-003": 0, "ITEM-004": 25})
    return jsonify({"reset": True, "inventory": dict(inventory_db)})


@app.route("/inventory/health", methods=["GET"])
def health():
    return jsonify({
        "service": "inventory",
        "status": "up",
        "bulkhead": {
            "check_pool_size": CHECK_POOL_SIZE,
            "reserve_pool_size": RESERVE_POOL_SIZE,
            "check_active": check_active,
            "reserve_active": reserve_active,
        }
    })


# ─── Fault Injection Admin Endpoints ─────────────────────────────────────────
@app.route("/fault/latency", methods=["POST"])
def inject_latency():
    body = request.json or {}
    fault_state["inject_latency"] = body.get("enabled", False)
    fault_state["latency_ms"]     = body.get("latency_ms", 5000)
    logger.warning(f"[FAULT] Latency {fault_state['latency_ms']}ms")
    return jsonify({"fault": "latency", "state": fault_state})


@app.route("/fault/crash", methods=["POST"])
def inject_crash():
    body = request.json or {}
    fault_state["inject_crash"] = body.get("enabled", False)
    return jsonify({"fault": "crash", "enabled": fault_state["inject_crash"]})


@app.route("/fault/exhaust", methods=["POST"])
def inject_exhaust():
    """Exhaust the thread pool to test bulkhead rejection."""
    body = request.json or {}
    fault_state["exhaust_pool"] = body.get("enabled", False)
    logger.warning(f"[FAULT] Pool exhaustion: {fault_state['exhaust_pool']}")
    return jsonify({"fault": "exhaust_pool", "enabled": fault_state["exhaust_pool"]})


@app.route("/fault/reset", methods=["POST"])
def reset_faults():
    fault_state.update({"inject_latency": False, "latency_ms": 0, "inject_crash": False, "exhaust_pool": False})
    return jsonify({"fault": "reset"})


@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=False)
