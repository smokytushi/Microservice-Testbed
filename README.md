# E-Commerce Microservice Fault Tolerance Testbed
**FYP Project P237 — Fault Tolerance in Microservices Framework**

A lightweight testbed for evaluating fault tolerance mechanisms in a microservice architecture.
Built as the prototype for Phase 3 of the FYP research methodology.

---

## Architecture Overview

```
Client Request
      │
      ▼
┌─────────────────┐
│  Order Service  │  ← Timeout Mechanism        :5000
│   (Entry Point) │
└────────┬────────┘
         │ calls in sequence
    ┌────┼────────────────┐
    ▼    ▼                ▼
┌────────┐  ┌──────────┐  ┌──────────────┐
│Payment │  │Inventory │  │Notification  │
│Service │  │Service   │  │Service       │
│(Circuit│  │(Bulkhead)│  │(Retry+Backoff│
│Breaker)│  │          │  │)             │
└────────┘  └──────────┘  └──────────────┘
     :5001       :5002           :5003

Observability Stack:
  Jaeger   → http://localhost:16686   (Distributed Tracing)
  Grafana  → http://localhost:3000    (Metrics Dashboard, login: admin/testbed)
  Prometheus→ http://localhost:9090   (Raw Metrics)
```

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Docker | 24+ | https://docs.docker.com/get-docker/ |
| Docker Compose | v2+ | Included with Docker Desktop |
| Python | 3.9+ | For running test scripts only |

---

## Quick Start (5 minutes)

### Step 1 — Clone / Set up directory
```bash
# Your project should have this structure:
ecommerce-testbed/
├── order-service/
├── payment-service/
├── inventory-service/
├── notification-service/
├── observability/
├── configs/
├── scripts/
└── docker-compose.yml
```

### Step 2 — Start the testbed
```bash
cd ecommerce-testbed
docker compose up --build
```

Wait ~60 seconds for all services to start. You will see logs from all 4 services.

### Step 3 — Verify all services are up
```bash
python scripts/health_check.py
```

Expected output:
```
── Testbed Health Check ─────────────────────────────────────────
  Order Service                ✓ UP
  Payment Service              ✓ UP  CB=CLOSED
  Inventory Service            ✓ UP
  Notification Service         ✓ UP
  Jaeger UI                    ✓ UP
  Prometheus                   ✓ UP
  Grafana                      ✓ UP
```

### Step 4 — Send a test order
```bash
curl -X POST http://localhost:5000/order \
  -H "Content-Type: application/json" \
  -d '{"item_id": "ITEM-001", "quantity": 1, "amount": 50.0}'
```

---

## Running Fault Injection Tests

### Install test script dependencies
```bash
pip install requests
```

### Run all 5 scenarios (Static config)
```bash
python scripts/fault_injection.py --scenario all --config static
```

### Run a single scenario
```bash
python scripts/fault_injection.py --scenario s1 --config static
python scripts/fault_injection.py --scenario s2 --config static
python scripts/fault_injection.py --scenario s3 --config static
python scripts/fault_injection.py --scenario s4 --config static
python scripts/fault_injection.py --scenario s5 --config baseline
```

Results are saved to `results/test_results.csv`.

---

## Three Test Configurations

Each configuration maps to one of the three experimental setups in Phase 5:

### Config 1 — Baseline (no fault tolerance)
```bash
docker compose -f docker-compose.yml -f configs/baseline.yml up --build
python scripts/fault_injection.py --scenario all --config baseline
```

### Config 2 — Static (fixed thresholds)
```bash
docker compose -f docker-compose.yml -f configs/static.yml up --build
python scripts/fault_injection.py --scenario all --config static
```

### Config 3 — Dynamic (adaptive/tighter thresholds)
```bash
docker compose -f docker-compose.yml -f configs/dynamic.yml up --build
python scripts/fault_injection.py --scenario all --config dynamic
```

---

## Fault Scenarios

| ID | Name | Target | Fault | Mechanism Tested |
|----|------|--------|-------|-----------------|
| S1 | Payment Crash | Payment | 500 errors | Circuit Breaker |
| S2 | Inventory Latency | Inventory | 5s sleep | Timeout |
| S3 | Pool Exhaustion | Inventory | Thread hold | Bulkhead |
| S4 | Transient Failures | Notification | 80% random fail | Retry + Backoff |
| S5 | Cascading Failure | Payment + Inventory | Both crash + latency | All mechanisms |

---

## Manual Fault Injection via API

You can inject faults manually using curl for live observation in Jaeger/Grafana.

### Payment Service — Circuit Breaker
```bash
# Trigger crash (circuit breaker should open after 3 failures)
curl -X POST http://localhost:5001/fault/crash -H "Content-Type: application/json" -d '{"enabled": true}'

# Set random failure rate (0.0–1.0)
curl -X POST http://localhost:5001/fault/fail_rate -H "Content-Type: application/json" -d '{"rate": 0.7}'

# Check circuit breaker state
curl http://localhost:5001/payment/health

# Reset
curl -X POST http://localhost:5001/fault/reset
curl -X POST http://localhost:5001/fault/cb/reset
```

### Inventory Service — Bulkhead / Latency
```bash
# Inject 5 second latency
curl -X POST http://localhost:5002/fault/latency -H "Content-Type: application/json" -d '{"enabled": true, "latency_ms": 5000}'

# Exhaust thread pool
curl -X POST http://localhost:5002/fault/exhaust -H "Content-Type: application/json" -d '{"enabled": true}'

# View current stock
curl http://localhost:5002/inventory/stock

# Reset
curl -X POST http://localhost:5002/fault/reset
curl -X POST http://localhost:5002/inventory/stock/reset
```

### Notification Service — Retry
```bash
# Set 80% transient failure rate
curl -X POST http://localhost:5003/fault/transient -H "Content-Type: application/json" -d '{"rate": 0.8}'

# Reset
curl -X POST http://localhost:5003/fault/reset
```

### Order Service — Latency
```bash
# Inject 3s latency at entry point
curl -X POST http://localhost:5000/fault/latency -H "Content-Type: application/json" -d '{"enabled": true, "latency_ms": 3000}'

# Reset ALL faults across all services
curl -X POST http://localhost:5000/fault/reset
curl -X POST http://localhost:5001/fault/reset
curl -X POST http://localhost:5002/fault/reset
curl -X POST http://localhost:5003/fault/reset
```

---

## Observability — What to Look For

### Jaeger Traces (http://localhost:16686)
1. Select service: `order-service`
2. Click **Find Traces**
3. Open a trace to see the full span tree across all 4 services
4. During fault injection, look for:
   - Red spans (errors)
   - Long spans (latency)
   - Missing child spans (circuit open / timeout)

### Grafana Dashboard (http://localhost:3000)
Login: `admin` / `testbed`

Key panels to watch during tests:
- `order_requests_total` by status — success vs failure rate
- `circuit_breaker_state` — 0=closed, 1=open, 2=half-open
- `order_timeout_total` — timeout events per service
- `bulkhead_rejected_total` — rejected requests per pool
- `notification_retries_total` — retry attempts

### Prometheus (http://localhost:9090)
Useful queries:
```promql
# Error rate per service
rate(order_requests_total{status!="success"}[1m])

# Circuit breaker state over time
circuit_breaker_state

# Retry rate
rate(notification_retries_total[1m])

# Bulkhead rejections
rate(bulkhead_rejected_total[1m])
```

---

## Collecting Data for Phase 5 Comparative Analysis

Run the full test matrix:

```bash
# 1. Baseline
docker compose -f docker-compose.yml -f configs/baseline.yml up --build -d
sleep 30
python scripts/fault_injection.py --scenario all --config baseline

# 2. Static
docker compose -f docker-compose.yml -f configs/static.yml up --build -d
sleep 30
python scripts/fault_injection.py --scenario all --config static

# 3. Dynamic
docker compose -f docker-compose.yml -f configs/dynamic.yml up --build -d
sleep 30
python scripts/fault_injection.py --scenario all --config dynamic
```

Results CSV at `results/test_results.csv` contains:
- Scenario name and config
- Total requests, success count, error count
- Success rate %
- Average latency (ms)

Use this data directly for Phase 5 comparative tables and charts.

---

## Project File Structure

```
ecommerce-testbed/
├── order-service/
│   ├── app.py              ← Timeout mechanism
│   ├── requirements.txt
│   └── Dockerfile
├── payment-service/
│   ├── app.py              ← Circuit breaker
│   ├── requirements.txt
│   └── Dockerfile
├── inventory-service/
│   ├── app.py              ← Bulkhead isolation
│   ├── requirements.txt
│   └── Dockerfile
├── notification-service/
│   ├── app.py              ← Retry + backoff
│   ├── requirements.txt
│   └── Dockerfile
├── observability/
│   ├── prometheus.yml
│   └── grafana/
│       ├── datasources/prometheus.yml
│       └── dashboards/
├── configs/
│   ├── baseline.yml        ← Config 1: No fault tolerance
│   ├── static.yml          ← Config 2: Fixed thresholds
│   └── dynamic.yml         ← Config 3: Adaptive thresholds
├── scripts/
│   ├── fault_injection.py  ← Main test runner (5 scenarios)
│   ├── load_generator.py   ← Continuous load for baseline
│   └── health_check.py     ← Service health status
├── results/                ← CSV results (auto-created)
└── docker-compose.yml
```

---

## Stopping the Testbed

```bash
docker compose down          # stop containers
docker compose down -v       # stop + remove volumes
```
