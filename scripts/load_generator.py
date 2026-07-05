#!/usr/bin/env python3
"""
load_generator.py — Continuous Load Generator
==============================================
Sends continuous traffic to the Order service for baseline measurement.

Usage:
    python scripts/load_generator.py --rps 2 --duration 60
"""

import argparse
import time
import threading
import requests
from datetime import datetime

BASE_URL = "http://localhost:5000"
stats = {"total": 0, "success": 0, "error": 0, "total_ms": 0.0}
lock  = threading.Lock()


def send_order():
    start = time.time()
    try:
        r = requests.post(f"{BASE_URL}/order",
                          json={"item_id": "ITEM-001", "quantity": 1, "amount": 25.0},
                          timeout=8)
        ms = (time.time() - start) * 1000
        with lock:
            stats["total"] += 1
            stats["total_ms"] += ms
            if r.status_code == 200:
                stats["success"] += 1
            else:
                stats["error"] += 1
    except Exception:
        ms = (time.time() - start) * 1000
        with lock:
            stats["total"] += 1
            stats["error"] += 1
            stats["total_ms"] += ms


def print_stats():
    with lock:
        t = stats["total"]
        s = stats["success"]
        e = stats["error"]
        avg = stats["total_ms"] / t if t else 0
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] Sent={t:4d}  Success={s:4d}  Error={e:4d}  "
          f"Rate={round(s/t*100,1) if t else 0:5.1f}%  Avg={avg:6.1f}ms")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rps",      type=float, default=1.0,  help="Requests per second")
    parser.add_argument("--duration", type=int,   default=60,   help="Duration in seconds")
    args = parser.parse_args()

    interval = 1.0 / args.rps
    end_time = time.time() + args.duration

    print(f"Load generator: {args.rps} req/s for {args.duration}s → {BASE_URL}")
    print("-" * 70)

    last_print = time.time()
    while time.time() < end_time:
        t = threading.Thread(target=send_order)
        t.daemon = True
        t.start()
        time.sleep(interval)
        if time.time() - last_print >= 5:
            print_stats()
            last_print = time.time()

    time.sleep(2)
    print("\n── Final Stats ──")
    print_stats()
