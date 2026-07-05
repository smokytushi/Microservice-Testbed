#!/usr/bin/env python3
"""
fault_injection.py — Fault Injection Test Runner
=================================================
Runs the 5 fault scenarios from the FYP research against the testbed.
Each scenario can run across 3 configurations (baseline, static, dynamic).

Usage:
    python scripts/fault_injection.py --scenario all
    python scripts/fault_injection.py --scenario s1 --config static
    python scripts/fault_injection.py --scenario s3
"""

import argparse
import time
import json
import requests
import threading
import csv
from datetime import datetime

# Service URLs 
BASE = {
    "order":    "http://localhost:5000",
    "payment":  "http://localhost:5001",
    "inventory":"http://localhost:5002",
    "notify":   "http://localhost:5003",
}

RESULTS = []
results_lock = threading.Lock()


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] {msg}")


# Helpers
def reset_all_faults():
    """Reset all injected faults across all services."""
    for svc, url in BASE.items():
        try:
            requests.post(f"{url}/fault/reset", timeout=3)
        except Exception:
            pass
    log(" All faults reset")


def reset_inventory():
    requests.post(f"{BASE['inventory']}/inventory/stock/reset", timeout=3)
    log(" Inventory stock reset")


def reset_circuit_breaker():
    requests.post(f"{BASE['payment']}/fault/cb/reset", timeout=3)
    log(" Circuit breaker reset")


def send_order(item_id="ITEM-001", quantity=1, amount=50.0):
    """Send one order and return (status_code, response_json, duration_ms)."""
    start = time.time()
    try:
        r = requests.post(
            f"{BASE['order']}/order",
            json={"item_id": item_id, "quantity": quantity, "amount": amount},
            timeout=10
        )
        duration = (time.time() - start) * 1000
        return r.status_code, r.json(), round(duration, 2)
    except requests.exceptions.Timeout:
        duration = (time.time() - start) * 1000
        return 504, {"error": "timeout"}, round(duration, 2)
    except Exception as e:
        duration = (time.time() - start) * 1000
        return 503, {"error": str(e)}, round(duration, 2)


def send_load(n=10, delay=0.3, label=""):
    """Send n orders, collect results."""
    results = []
    for i in range(n):
        code, resp, duration = send_order()
        results.append({"req": i+1, "status": code, "duration_ms": duration,
                         "response": resp, "label": label})
        log(f"  [{label}] #{i+1}: HTTP {code} | {duration}ms")
        time.sleep(delay)
    return results


def record(scenario, config, results):
    success = sum(1 for r in results if r["status"] == 200)
    errors  = len(results) - success
    avg_ms  = sum(r["duration_ms"] for r in results) / len(results) if results else 0
    with results_lock:
        RESULTS.append({
            "scenario": scenario,
            "config": config,
            "total_requests": len(results),
            "success": success,
            "errors": errors,
            "success_rate": round(success / len(results) * 100, 1) if results else 0,
            "avg_latency_ms": round(avg_ms, 2),
            "timestamp": datetime.now().isoformat()
        })
    log(f"\n  ── Summary [{scenario} / {config}]: "
        f"Success={success}/{len(results)} ({round(success/len(results)*100,1)}%) "
        f"Avg={round(avg_ms,2)}ms\n")


# SCENARIO S1: Payment Service Crash (tests Circuit Breaker)
def scenario_s1(config="baseline"):
    log(f"\n{'='*60}")
    log(f"SCENARIO S1: Payment Service Crash [{config}]")
    log(f"Expects: Circuit Breaker to open and protect Order service")
    log(f"{'='*60}")
    reset_all_faults()
    reset_circuit_breaker()

    log("Phase 1: Steady state (5 requests)...")
    baseline = send_load(5, delay=0.5, label="steady")

    log("\nPhase 2: Injecting payment crash...")
    requests.post(f"{BASE['payment']}/fault/crash", json={"enabled": True})
    log("   Payment crash active")

    log("\nPhase 3: Load under fault (10 requests)...")
    fault_results = send_load(10, delay=0.3, label="fault")

    log("\nPhase 4: Removing fault — observing recovery...")
    requests.post(f"{BASE['payment']}/fault/reset")
    reset_circuit_breaker()
    time.sleep(1)
    recovery = send_load(5, delay=0.5, label="recovery")

    all_results = baseline + fault_results + recovery
    record("S1_payment_crash", config, all_results)
    return all_results


# SCENARIO S2: Inventory Latency (tests Order Service Timeout) 
def scenario_s2(config="baseline"):
    log(f"\n{'='*60}")
    log(f"SCENARIO S2: Inventory Latency Injection [{config}]")
    log("Expects: Order service timeout to fire, return 503 gracefully")
    log(f"{'='*60}")
    reset_all_faults()

    log("Phase 1: Steady state...")
    baseline = send_load(5, delay=0.5, label="steady")

    log("\nPhase 2: Injecting 5s latency on inventory...")
    requests.post(f"{BASE['inventory']}/fault/latency", json={"enabled": True, "latency_ms": 5000})
    log("   5000ms latency active on inventory")

    log("\nPhase 3: Load under fault...")
    fault_results = send_load(8, delay=0.5, label="fault")

    log("\nPhase 4: Recovery...")
    requests.post(f"{BASE['inventory']}/fault/reset")
    time.sleep(0.5)
    recovery = send_load(5, delay=0.5, label="recovery")

    all_results = baseline + fault_results + recovery
    record("S2_inventory_latency", config, all_results)
    return all_results


# SCENARIO S3: Inventory Pool Exhaustion (tests Bulkhead) 
def scenario_s3(config="baseline"):
    log(f"\n{'='*60}")
    log(f"SCENARIO S3: Inventory Pool Exhaustion [{config}]")
    log(f"Expects: Bulkhead rejects overflow requests, other services unaffected")
    log(f"{'='*60}")
    reset_all_faults()

    log("Phase 1: Steady state...")
    baseline = send_load(5, delay=0.5, label="steady")

    log("\nPhase 2: Exhausting inventory thread pool...")
    requests.post(f"{BASE['inventory']}/fault/exhaust", json={"enabled": True})
    log("   Pool exhaustion active")

    log("\nPhase 3: Concurrent load (tests bulkhead rejection)...")

    def fire_order(i, results):
        code, resp, duration = send_order()
        results.append({"req": i, "status": code, "duration_ms": duration, "response": resp, "label": "fault"})
        log(f"  [fault] #{i}: HTTP {code} | {duration}ms")

    threads = []
    thread_results = []
    for i in range(15):
        t = threading.Thread(target=fire_order, args=(i+1, thread_results))
        threads.append(t)

    for t in threads:
        t.start()
        time.sleep(0.1)
    for t in threads:
        t.join()

    log("\nPhase 4: Recovery...")
    requests.post(f"{BASE['inventory']}/fault/reset")
    time.sleep(0.5)
    recovery = send_load(5, delay=0.5, label="recovery")

    all_results = baseline + thread_results + recovery
    record("S3_pool_exhaustion", config, all_results)
    return all_results


# SCENARIO S4: Notification Transient Failures (tests Retry) 
def scenario_s4(config="baseline"):
    log(f"\n{'='*60}")
    log(f"SCENARIO S4: Notification Transient Failures [{config}]")
    log(f"Expects: Retry+backoff recovers, orders still succeed (notification non-critical)")
    log(f"{'='*60}")
    reset_all_faults()

    log("Phase 1: Steady state...")
    baseline = send_load(5, delay=0.5, label="steady")

    log("\nPhase 2: Injecting 80% transient failure rate on notification...")
    requests.post(f"{BASE['notify']}/fault/transient", json={"rate": 0.8})

    log("\nPhase 3: Load under transient fault...")
    fault_results = send_load(10, delay=0.5, label="fault")

    log("\nPhase 4: Recovery...")
    requests.post(f"{BASE['notify']}/fault/reset")
    recovery = send_load(5, delay=0.5, label="recovery")

    all_results = baseline + fault_results + recovery
    record("S4_notification_transient", config, all_results)
    return all_results


# SCENARIO S5: Cascading Failure (Baseline — no protection) 
def scenario_s5(config="baseline"):
    log(f"\n{'='*60}")
    log(f"SCENARIO S5: Cascading Failure (No Protection) [{config}]")
    log(f"Expects: Payment crash propagates through entire order chain")
    log(f"{'='*60}")
    reset_all_faults()
    reset_circuit_breaker()

    log("Phase 1: Steady state...")
    baseline = send_load(5, delay=0.5, label="steady")

    log("\nPhase 2: Injecting payment crash AND inventory latency simultaneously...")
    requests.post(f"{BASE['payment']}/fault/crash",    json={"enabled": True})
    requests.post(f"{BASE['inventory']}/fault/latency", json={"enabled": True, "latency_ms": 4000})
    log("   Multiple faults active — observing cascading behaviour")

    log("\nPhase 3: Load under compound fault...")
    fault_results = send_load(10, delay=0.3, label="cascade")

    log("\nPhase 4: Recovery from cascade...")
    reset_all_faults()
    reset_circuit_breaker()
    time.sleep(2)
    recovery = send_load(5, delay=0.5, label="recovery")

    all_results = baseline + fault_results + recovery
    record("S5_cascading_failure", config, all_results)
    return all_results


# Save Results
def save_results(filename="results/test_results.csv"):
    import os
    os.makedirs("results", exist_ok=True)
    if not RESULTS:
        return
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RESULTS[0].keys())
        writer.writeheader()
        writer.writerows(RESULTS)
    log(f"\n Results saved to {filename}")
    print("\n── FULL SUMMARY ──────────────────────────────────────────")
    print(f"{'Scenario':<30} {'Config':<12} {'Success%':<12} {'Avg(ms)':<12} {'Total'}")
    print("-" * 75)
    for r in RESULTS:
        print(f"{r['scenario']:<30} {r['config']:<12} {r['success_rate']:<12} {r['avg_latency_ms']:<12} {r['total_requests']}")


# Main 
SCENARIOS = {"s1": scenario_s1, "s2": scenario_s2, "s3": scenario_s3,
             "s4": scenario_s4, "s5": scenario_s5}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fault Injection Test Runner")
    parser.add_argument("--scenario", default="all",
                        choices=list(SCENARIOS.keys()) + ["all"],
                        help="Scenario to run (default: all)")
    parser.add_argument("--config", default="static",
                        choices=["baseline", "static", "dynamic"],
                        help="Config label for results (default: static)")
    args = parser.parse_args()

    log(f"Starting testbed fault injection — scenario={args.scenario} config={args.config}")
    log("Checking service health...")

    for svc, url in BASE.items():
        try:
            r = requests.get(f"{url}/{svc.split('-')[0]}/health" if svc != "notify" else f"{url}/notify/health", timeout=3)
            log(f"   {svc}: {r.status_code}")
        except Exception:
            log(f"  ✗ {svc}: unreachable — is the testbed running? (docker compose up)")

    print()
    if args.scenario == "all":
        for name, fn in SCENARIOS.items():
            fn(config=args.config)
            time.sleep(2)
    else:
        SCENARIOS[args.scenario](config=args.config)

    save_results()
