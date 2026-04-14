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
  --rounds=3 \
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
## License

This software is licensed as [Apache 2.0](LICENSE).
