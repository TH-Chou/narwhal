# Novel DAG

[![build status](https://img.shields.io/github/actions/workflow/status/asonnino/narwhal/rust.yml?branch=master&logo=github&style=flat-square)](https://github.com/asonnino/narwhal/actions)
[![rustc](https://img.shields.io/badge/rustc-1.51+-blue?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![python](https://img.shields.io/badge/python-3.9-blue?style=flat-square&logo=python&logoColor=white)](https://www.python.org/downloads/release/python-390/)
[![license](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](LICENSE)

This repository is a research-oriented DAG consensus codebase derived from Narwhal/Tusk and adapted for protocol experimentation.

- Implementation language: Rust
- Benchmark/automation scripts: Python + Fabric
- Core crates: `primary`, `consensus`, `worker`, `node`, `network`, `crypto`, `store`, `config`

The code is intended for experimentation and benchmarking, not production deployment.

## Quick Start

### 1) Prerequisites

- Rust toolchain (`cargo`, `rustc`)
- Python 3 + `pip`
- `clang` (required by RocksDB)
- `tmux` (used by benchmark scripts)

### 2) Build and test

From repository root:

```bash
cargo build
cargo test
```

### 3) Run local benchmark

```bash
cd benchmark
pip install -r requirements.txt
fab local
```

You can also run protocol comparison tasks from `benchmark/fabfile.py`, for example:

```bash
fab compare-consensus-groups --duration=30 --rounds=5 --rate=50000 --output-csv=results/my_avg.csv --output-runs-csv=results/my_runs.csv
```

## Run a node manually

`node` provides the executable entrypoint.

```bash
cargo run -p node -- --help
```

Key subcommands:

- `generate_keys`
- `run primary`
- `run worker --id <INT>`

See `node/src/main.rs` for full CLI flags and required config files.

## Docs

- Benchmark guide: `benchmark/README.md`
- Primary module notes: `primary/README.md`
- Worker module notes: `worker/README.md`
- Protocol notes for this repo: `new DAG structure.md`

## Aws

source /Users/apple/Documents/narwhal/NovelDAG/benchmark/.venv310/bin/activate

fab create --nodes=2
fab start

fab info
fab install
fab kill

python -u /Users/apple/Documents/narwhal/.test/run_triple_aws_batch_benchmark.py \
  --nodes=10 \
  --faults=1,3 \
  --rate-start=20000 \
  --rate-step=20000 \
  --rate-end=220000 \
  --rounds=2 \
  --duration=20 \
  --out-dir=triple_aws_batch_streamed \
  --fab-bin=/Users/apple/Documents/narwhal/NovelDAG/benchmark/.venv310/bin/fab \
  --batch-prefix=tripleaws-streamed \
  --phase=all \
  --remote-retries=5 \
  --retry-delay=15 \
  --collect-poll-seconds=120

fab stop
fab destroy

## Gcp

在三套 benchmark 的 `settings.json` 中把 provider 切换为 gcp（`NovelDAG/benchmark/settings.json`、`Narwhal/benchmark/settings.json`、`Bullshark/benchmark/settings.json`）：

```json
{
  "provider": "gcp",
  "key": {
    "name": "gcp_dag_rsa",
    "path": "/Users/apple/.ssh/gcp_dag_rsa"
  },
  "port": 5000,
  "repo": {
    "name": "narwhal",
    "url": "https://github.com/TH-Chou/narwhal.git",
    "branch": "master"
  },
  "instances": {
    "type": "n2-standard-2",
    "project": "<YOUR_GCP_PROJECT>",
    "zones": ["us-central1-a", "europe-west1-b", "asia-southeast1-b"],
    "network": "default",
    "image_project": "ubuntu-os-cloud",
    "image_family": "ubuntu-2204-lts",
    "disk_size_gb": 200
  }
}
```

然后和 AWS 一样使用 `fab create/start/info/install/kill/stop/destroy`，命令不变：

```bash
source /Users/apple/Documents/narwhal/NovelDAG/benchmark/.venv310/bin/activate

fab create --nodes=2
fab start
fab info
fab install
fab kill
```

批量三协议脚本可直接走 GCP 入口（功能与 AWS 脚本一致）：

```bash
python -u /Users/apple/Documents/narwhal/.test/run_triple_gcp_batch_benchmark.py \
  --nodes=10 \
  --faults=1,3 \
  --rate-start=20000 \
  --rate-step=20000 \
  --rate-end=220000 \
  --rounds=2 \
  --duration=20 \
  --out-dir=triple_gcp_batch_streamed \
  --fab-bin=/Users/apple/Documents/narwhal/NovelDAG/benchmark/.venv310/bin/fab \
  --batch-prefix=triplegcp-streamed \
  --phase=all \
  --remote-retries=5 \
  --retry-delay=15 \
  --collect-poll-seconds=120
```

只想用同一脚本也可以：`run_triple_aws_batch_benchmark.py --cloud=gcp ...`。

## License

This software is licensed as [Apache 2.0](LICENSE).
