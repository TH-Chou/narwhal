#!/usr/bin/env python3
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


def main():
    base = Path(__file__).resolve().parent / "comparison_results" / "common_coin_n16_f0_f5"
    avg_path = base / "average.csv"

    rows = []
    with avg_path.open("r", newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(
                {
                    "project": r["project"],
                    "faults": int(r["faults"]),
                    "rate": int(r["rate"]),
                    "consensus_tps": float(r["consensus_tps"]),
                    "consensus_latency_ms": float(r["consensus_latency_ms"]),
                }
            )

    styles = {
        ("NovelDAG", 0): ("-o", "NovelDAG f=0"),
        ("NovelDAG", 5): ("--o", "NovelDAG f=5"),
        ("Narwhal", 0): ("-s", "Narwhal f=0"),
        ("Narwhal", 5): ("--s", "Narwhal f=5"),
        ("narwhal", 0): ("-s", "narwhal f=0"),
        ("narwhal", 5): ("--s", "narwhal f=5"),
    }

    series = defaultdict(list)
    for r in rows:
        series[(r["project"], r["faults"])].append(r)

    plt.figure(figsize=(10, 6))
    for key, points in sorted(series.items()):
        points = sorted(points, key=lambda x: x["rate"])
        x = [p["rate"] for p in points]
        y = [p["consensus_tps"] for p in points]
        style, label = styles.get(key, ("-^", f"{key[0]} f={key[1]}"))
        plt.plot(x, y, style, label=label)

    plt.xlabel("Injection Rate (tx/s)")
    plt.ylabel("Consensus TPS (tx/s)")
    plt.title("Common Coin: Injection Rate vs Consensus TPS")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(base / "rate_vs_tps_common_coin.png", dpi=180)
    plt.close()

    plt.figure(figsize=(10, 6))
    for key, points in sorted(series.items()):
        points = sorted(points, key=lambda x: x["rate"])
        x = [p["consensus_tps"] for p in points]
        y = [p["consensus_latency_ms"] for p in points]
        style, label = styles.get(key, ("-^", f"{key[0]} f={key[1]}"))
        plt.plot(x, y, style, label=label)

    plt.xlabel("Consensus TPS (tx/s)")
    plt.ylabel("Consensus Latency (ms)")
    plt.title("Common Coin: Consensus TPS vs Latency")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(base / "tps_vs_latency_common_coin.png", dpi=180)
    plt.close()

    print("replot done")


if __name__ == "__main__":
    main()
