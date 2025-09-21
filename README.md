## Deltakit - Delta Lake library in Rust

Deltakit is a CLI and library for those working on training systems and working with data in Delta Lake. For fast diffs, counts, compaction planning, partition health, manifests, and safety checks.

The kit contains a shard planner which does deterministic, versioned shard assignment of active files (or row-groups) into K shards optimized for balanced bytes or rows with constraints (e.g. co-locate by partitions, max files/shard, sticky assignment across versions). Distributed training and large batch jobs want stable work distribution, resumability, and little to no churning across versions. 

## features
- **ls**, **diff**, **rowcount**, **partition‑health**, **manifest** (emit a stable file list for external readers (Trino/Presto/filelist/Hive)), **vacuum‑dry‑run** (orphans vs referenced files and safety flags), **snapshot**, **shard‑manifest** (deterministic K‑shard planner for training or batching). also: `zorder-plan`, `schema-guard`, `drift`, `footprint`, `dedupe-plan`.

## installation
```bash
curl https://sh.rustup.rs -sSf | sh -s -- -y

cargo build

cargo build -F s3
cargo build -F gcs
cargo build -F azure

# optional: to install the deltakit cli
cargo install --path crates/deltakit-cli
```

## quickstart
```bash
# table inspection
./target/debug/deltakit ls /data/delta/my_table

# json output
./target/debug/deltakit diff s3://bucket/table --from 101 --to 108 --json | jq .

# partition aware compaction plan, dry run
./target/debug/deltakit compact-plan /data/delta/my_table --target 256 --by dt

# vacuum safety auditor, no deletes
./target/debug/deltakit vacuum-dry-run s3://bucket/table --retention 7

# manifest of files for a specific version
./target/debug/deltakit manifest /data/delta/my_table --version 432 --format trino

# materialize a stable file list
./target/debug/deltakit snapshot /data/delta/my_table --version 432 --out files.txt

# deterministic shard manifest for training/batch
./target/debug/deltakit shard-manifest /data/delta/my_table --version 432 --shards 64 --by dt --sticky-by dt --balance bytes --json | jq .
```

## CLI usage
Global flags apply to all commands:
- `--json`: for machine‑readable output
- `--quiet`: to suppress human log

read‑only commands:
- `deltakit ls <uri>`
- `deltakit diff <uri> --from <v1> --to <v2>`
- `deltakit rowcount <uri> [--by dt,country] [--version N]`
- `deltakit compact-plan <uri> --target 256 [--by dt]`
- `deltakit partition-health <uri> --by dt,country`
- `deltakit manifest <uri> --version N --format trino|hive|presto|filelist`
- `deltakit snapshot <uri> --version N --out files.txt`

### output schemas (stable JSON)
- `ls`: `{ uri, version, files, bytes, partitions[] }`
- `diff`: `{ from, to, files_added, files_removed, bytes_added, bytes_removed }`
- `rowcount`: `[ { group: { key->value }, rows } ]`
- `compact-plan`: `{ target_file_size_bytes, partition_by[], groups: [ { partition{}, input_files[], total_input_bytes } ], estimated_io_bytes }`
- `partition-health`: `{ by[], cardinality: [ { key, distinct } ], empty_partitions, total_files }`
- `manifest`: `{ version, files: [ { path, size } ] }`

## backends & auth
- **Local filesystem**: default; no feature flags required
- **S3**: build with `-F s3`; uses standard AWS env/credential chain
- **GCS**: build with `-F gcs`; uses Application Default Credentials
- **Azure**: build with `-F azure`

environment hints honored when applicable:
- `AWS_PROFILE`, `AWS_ROLE_ARN`, `AWS_REGION`

## general notes
- Entire tool is read‑only. 
- Designed to run routinely in prod without modifying tables
- We prefer snapshot/log inspection over distributed scans

Workspace crates:
- `storage`: thin `object_store` setup (local + optional S3/GCS/Azure), retries/convenience, path utils
- `cli-core`: global flags, logging/formatting helpers
- `deltakit-core`: read‑only helpers over Delta tables (diffs, counts, plans, manifests, vacuum safety)
- `deltakit-cli`: CLI wiring to core with human/JSON output

## testing
Run the full suite:
```bash
cargo test
```
highlights:
- synthesizes a small local Delta log and verifies all the e2e flows (diff, rowcount, partition health, compaction, manifest, vacuum safety)
- storage unit tests to validate URI parsing