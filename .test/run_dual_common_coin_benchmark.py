#!/usr/bin/env python3
import csv
import json
import subprocess
import sys
from pathlib import Path


def find_project_dirs(root: Path):
    def is_valid_project(path: Path) -> bool:
        return (
            path.exists()
            and (path / "Cargo.toml").exists()
            and (path / "benchmark" / "benchmark" / "local.py").exists()
            and (path / "primary" / "src" / "lib.rs").exists()
        )

    preferred = [
        ("NovelDAG", "NovelDAG", "common_coin"),
        ("Narwhal", "Narwhal", "common_coin"),
        ("narwhal", "Narwhal", "common_coin"),
        ("Bullshark", "Bullshark", "round_robin"),
    ]
    found = []
    seen_canonical = set()
    for folder_name, project_name, protocol in preferred:
        if project_name in seen_canonical:
            continue
        name = folder_name
        path = root / name
        if is_valid_project(path):
            found.append((project_name, path, protocol))
            seen_canonical.add(project_name)

    return found


def run_one(
    benchmark_dir: Path,
    nodes: int,
    faults: int,
    rate: int,
    duration: int,
    node_parameters: dict,
    protocol_label: str,
):
    inline = r'''
import json
import re
from benchmark.local import LocalBench
bench = {
    "faults": __FAULTS__,
    "nodes": __NODES__,
    "workers": 1,
    "rate": __RATE__,
    "tx_size": 512,
    "duration": __DURATION__,
}
node = {
__NODE_PARAMETERS__
}
parser = LocalBench(bench, node).run(False)
text = parser.result()
def pick(label):
    m = re.search(rf"{label}:\s*([0-9,\.]+)", text)
    if not m:
        raise RuntimeError(f"cannot parse '{label}' from benchmark output")
    return float(m.group(1).replace(",", ""))
metrics = {
    "consensus_protocol": "__PROTOCOL_LABEL__",
    "consensus_tps": pick("Consensus TPS"),
    "consensus_latency_ms": pick("Consensus latency"),
    "end_to_end_tps": pick("End-to-end TPS"),
    "end_to_end_latency_ms": pick("End-to-end latency"),
}
print("METRICS_JSON=" + json.dumps(metrics, ensure_ascii=False))
'''.replace("__FAULTS__", str(faults)).replace("__NODES__", str(nodes)).replace("__RATE__", str(rate)).replace("__DURATION__", str(duration)).replace("__NODE_PARAMETERS__", json.dumps(node_parameters, ensure_ascii=False)[1:-1]).replace("__PROTOCOL_LABEL__", protocol_label)

    proc = subprocess.run(
        ["python3", "-c", inline],
        cwd=str(benchmark_dir),
        text=True,
        capture_output=True,
    )

    stdout = proc.stdout or ""
    stderr = proc.stderr or ""
    if proc.returncode != 0:
        raise RuntimeError(
            f"Benchmark failed in {benchmark_dir}\n"
            f"exit={proc.returncode}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        )

    marker = "METRICS_JSON="
    metrics_line = None
    for line in stdout.splitlines()[::-1]:
        if line.startswith(marker):
            metrics_line = line[len(marker):]
            break
    if metrics_line is None:
        raise RuntimeError(
            f"Could not parse metrics output in {benchmark_dir}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        )

    metrics = json.loads(metrics_line)
    return metrics, stdout


def aggregate(rows):
    grouped = {}
    for r in rows:
        key = (r["project"], r["faults"], r["rate"])
        grouped.setdefault(key, []).append(r)

    out = []
    for (project, faults, rate), items in sorted(grouped.items()):
        n = len(items)
        out.append(
            {
                "project": project,
                "faults": faults,
                "rate": rate,
                "consensus_tps": sum(x["consensus_tps"] for x in items) / n,
                "consensus_latency_ms": sum(x["consensus_latency_ms"] for x in items) / n,
                "end_to_end_tps": sum(x["end_to_end_tps"] for x in items) / n,
                "end_to_end_latency_ms": sum(x["end_to_end_latency_ms"] for x in items) / n,
                "runs": n,
            }
        )
    return out


def write_csv(path: Path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def append_csv_row(path: Path, row, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def load_existing_runs(path: Path):
    if not path.exists():
        return []

    rows = []
    with path.open("r", newline="", encoding="utf-8") as f:
        for raw in csv.DictReader(f):
            rows.append(
                {
                    "project": raw["project"],
                    "nodes": int(raw["nodes"]),
                    "faults": int(raw["faults"]),
                    "rate": int(raw["rate"]),
                    "duration": int(raw["duration"]),
                    "run": int(raw["run"]),
                    "consensus_protocol": raw.get("consensus_protocol", "common_coin"),
                    "consensus_tps": float(raw["consensus_tps"]),
                    "consensus_latency_ms": float(raw["consensus_latency_ms"]),
                    "end_to_end_tps": float(raw["end_to_end_tps"]),
                    "end_to_end_latency_ms": float(raw["end_to_end_latency_ms"]),
                }
            )
    return rows


def make_plots(avg_rows, out_dir: Path):
    try:
        import matplotlib.pyplot as plt
    except ModuleNotFoundError:
        print(
            "[WARN] matplotlib is not installed; skipping plot generation for now.",
            flush=True,
        )
        return False

    out_dir.mkdir(parents=True, exist_ok=True)
    plot_context = "batch_size=512KB, tx_size=512B, nodes=10"

    # Plot 1: Injection rate (x) vs consensus TPS (y)
    plt.figure(figsize=(10, 6))
    styles = {
        ("NovelDAG", 0): ("-o", "NovelDAG f=0"),
        ("NovelDAG", 5): ("--o", "NovelDAG f=5"),
        ("NovelDAG", 1): ("-o", "NovelDAG f=1"),
        ("NovelDAG", 3): ("--o", "NovelDAG f=3"),
        ("Narwhal", 0): ("-s", "Tusk f=0"),
        ("Narwhal", 5): ("--s", "Tusk f=5"),
        ("Narwhal", 1): ("-s", "Tusk f=1"),
        ("Narwhal", 3): ("--s", "Tusk f=3"),
        ("Bullshark", 1): ("-^", "Bullshark f=1"),
        ("Bullshark", 3): ("--^", "Bullshark f=3"),
        ("narwhal", 0): ("-s", "narwhal f=0"),
        ("narwhal", 5): ("--s", "narwhal f=5"),
    }

    series = {}
    for r in avg_rows:
        key = (r["project"], r["faults"])
        series.setdefault(key, []).append(r)

    for key, points in sorted(series.items()):
        points = sorted(points, key=lambda x: x["rate"])
        x = [p["rate"] for p in points]
        y = [p["consensus_tps"] for p in points]
        style, label = styles.get(key, ("-^", f"{key[0]} f={key[1]}"))
        plt.plot(x, y, style, label=label)

    plt.xlabel("Injection Rate (tx/s)")
    plt.ylabel("Consensus TPS (tx/s)")
    plt.title(f"Injection-TPS ({plot_context})")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_dir / "rate_vs_tps_common_coin.png", dpi=180)
    plt.close()

    # Plot 2: Consensus TPS (x) vs consensus latency (y)
    # Connect points by increasing injection rate to reflect load ramp.
    plt.figure(figsize=(10, 6))
    for key, points in sorted(series.items()):
        points = sorted(points, key=lambda x: x["rate"])
        x = [p["consensus_tps"] for p in points]
        y = [p["consensus_latency_ms"] for p in points]
        style, label = styles.get(key, ("-^", f"{key[0]} f={key[1]}"))
        plt.plot(x, y, style, label=label)

    plt.xlabel("Consensus TPS (tx/s)")
    plt.ylabel("Consensus Latency (ms)")
    plt.title(f"TPS-Latency ({plot_context})")
    plt.ylim(bottom=0)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_dir / "tps_vs_latency_common_coin.png", dpi=180)
    plt.close()

    return True


def main():
    script_root = Path(__file__).resolve().parent
    repo_root = script_root.parent
    projects = find_project_dirs(repo_root)
    if len(projects) < 3:
        print("Need three project folders: NovelDAG, Narwhal (or narwhal), and Bullshark", file=sys.stderr)
        sys.exit(1)

    print("Projects:")
    for name, path, protocol in projects:
        print(f"- {name}: {path} (protocol={protocol})")

    rates = list(range(20000, 280001, 20000))
    faults_list = [1, 3]
    rounds = 3
    duration = 30
    nodes = 10

    out_dir = script_root / "comparison_results" / "triple_n10_f1_f3_hdelay1000"
    runs_csv_path = out_dir / "runs.csv"
    avg_csv_path = out_dir / "average.csv"

    runs_fieldnames = [
        "project",
        "nodes",
        "faults",
        "rate",
        "duration",
        "run",
        "consensus_protocol",
        "consensus_tps",
        "consensus_latency_ms",
        "end_to_end_tps",
        "end_to_end_latency_ms",
    ]
    avg_fieldnames = [
        "project",
        "faults",
        "rate",
        "consensus_tps",
        "consensus_latency_ms",
        "end_to_end_tps",
        "end_to_end_latency_ms",
        "runs",
    ]

    existing_rows = load_existing_runs(runs_csv_path)
    target_rates = set(rates)
    target_faults = set(faults_list)
    target_projects = {project_name for project_name, _, _ in projects}
    rows = [
        r
        for r in existing_rows
        if r["project"] in target_projects
        and r["nodes"] == nodes
        and r["faults"] in target_faults
        and r["rate"] in target_rates
        and r["duration"] == duration
    ]
    dropped = len(existing_rows) - len(rows)
    if dropped > 0:
        print(
            f"Filtered out {dropped} stale entries from existing runs "
            f"(outside current sweep config)."
        )

    completed = {
        (r["project"], r["faults"], r["rate"], r["run"])
        for r in rows
    }

    if completed:
        print(f"Resuming from existing runs: {len(completed)} completed entries found.")

    plot_enabled = True

    project_runs = []
    for project_name, project_path, protocol_label in projects:
        node_parameters = {
            "header_size": 1000,
            "max_header_delay": 200,
            "gc_depth": 50,
            "sync_retry_delay": 10000,
            "sync_retry_nodes": 3,
            "batch_size": 500000,
            "max_batch_delay": 200,
        }
        if protocol_label in {"common_coin", "round_robin"} and project_name != "Bullshark":
            node_parameters["consensus_protocol"] = protocol_label

        project_runs.append((project_name, project_path / "benchmark", protocol_label, node_parameters))

    for faults in faults_list:
        for rate in rates:
            for run_idx in range(1, rounds + 1):
                for project_name, bench_dir, protocol_label, node_parameters in project_runs:
                    run_key = (project_name, faults, rate, run_idx)
                    if run_key in completed:
                        print(
                            f"[SKIP] project={project_name} nodes={nodes} faults={faults} "
                            f"rate={rate} duration={duration}s run={run_idx}/{rounds} (already persisted)",
                            flush=True,
                        )
                        continue

                    print(
                        f"[RUN] project={project_name} nodes={nodes} faults={faults} "
                        f"rate={rate} duration={duration}s run={run_idx}/{rounds}",
                        flush=True,
                    )
                    metrics, stdout = run_one(
                        bench_dir,
                        nodes,
                        faults,
                        rate,
                        duration,
                        node_parameters,
                        protocol_label,
                    )
                    row = {
                        "project": project_name,
                        "nodes": nodes,
                        "faults": faults,
                        "rate": rate,
                        "duration": duration,
                        "run": run_idx,
                        "consensus_protocol": metrics.get("consensus_protocol", "common_coin"),
                        "consensus_tps": metrics["consensus_tps"],
                        "consensus_latency_ms": metrics["consensus_latency_ms"],
                        "end_to_end_tps": metrics["end_to_end_tps"],
                        "end_to_end_latency_ms": metrics["end_to_end_latency_ms"],
                    }
                    rows.append(row)
                    completed.add(run_key)

                    append_csv_row(runs_csv_path, row, runs_fieldnames)
                    avg_rows = aggregate(rows)
                    write_csv(avg_csv_path, avg_rows, avg_fieldnames)
                    if plot_enabled:
                        plot_enabled = make_plots(avg_rows, out_dir)

                    print(
                        f"[FLUSHED] runs={len(rows)} latest={project_name} f={faults} rate={rate} run={run_idx}",
                        flush=True,
                    )

    avg_rows = aggregate(rows)

    write_csv(runs_csv_path, rows, runs_fieldnames)
    write_csv(avg_csv_path, avg_rows, avg_fieldnames)

    if plot_enabled:
        make_plots(avg_rows, out_dir)
    else:
        print("[INFO] Plots were skipped because matplotlib is unavailable.")

    print("\nDone.")
    print(f"CSV: {out_dir / 'runs.csv'}")
    print(f"CSV: {out_dir / 'average.csv'}")
    print(f"Plot: {out_dir / 'rate_vs_tps_common_coin.png'}")
    print(f"Plot: {out_dir / 'tps_vs_latency_common_coin.png'}")


if __name__ == "__main__":
    main()
