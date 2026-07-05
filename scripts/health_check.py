#!/usr/bin/env python3
"""
health_check.py — Quick health status of all testbed services
Usage: python scripts/health_check.py
"""
import requests

SERVICES = {
    "Order Service":        "http://localhost:5000/order/health",
    "Payment Service":      "http://localhost:5001/payment/health",
    "Inventory Service":    "http://localhost:5002/inventory/health",
    "Notification Service": "http://localhost:5003/notify/health",
    "Jaeger UI":            "http://localhost:16686",
    "Prometheus":           "http://localhost:9090/-/healthy",
    "Grafana":              "http://localhost:3000/api/health",
}

print("\n── Testbed Health Check ─────────────────────────────────────────")
for name, url in SERVICES.items():
    try:
        r = requests.get(url, timeout=3)
        status = " UP" if r.status_code < 400 else f"✗ HTTP {r.status_code}"
        try:
            detail = r.json()
            if "circuit_breaker" in detail:
                status += f"  CB={detail['circuit_breaker']['state']}"
        except Exception:
            pass
    except Exception as e:
        status = f"✗ UNREACHABLE ({type(e).__name__})"
    print(f"  {name:<28} {status}")
print()
