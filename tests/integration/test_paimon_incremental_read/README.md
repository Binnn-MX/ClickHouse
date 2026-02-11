## Paimon Incremental Read Integration Test (Scheme B)

This test validates Paimon incremental read with:

- A dedicated writer jar (`paimon-incremental-writer`) that creates and writes a normal Paimon table
- A ClickHouse table created by `ENGINE = PaimonLocal(...)`
- Incremental reads with Keeper state

### Test entry

- `test.py`

### Writer project

- `paimon-incremental-data/`
- Main class: `org.apache.paimon.rest.PaimonIncrementalWriter`
- Jar path expected by test:
  - `/root/paimon-incremental-data/target/paimon-incremental-writer-1.1.1.jar`

### Writer jar packaging

This test repository keeps the writer jar as split chunks in:

- `paimon-incremental-data/chunk_00`
- `paimon-incremental-data/chunk_01`
- `paimon-incremental-data/chunk_02`

At runtime, `test.py` copies the directory into the test container and assembles:

- `cat chunk_* > target/paimon-incremental-writer-1.1.1.jar`

If you rebuild the writer jar locally, re-split it into `chunk_*` before commit.

### Assertions covered

1. First incremental read after snapshot-1 returns `10`
2. Next read without new snapshot returns `0`
3. First incremental read after snapshot-2 returns `10`
4. Next read without new snapshot returns `0`
5. `paimon_target_snapshot_id = 1` is deterministic and returns `10` repeatedly
6. With `max_consume_snapshots = 2`, after 3 commits (10 rows per snapshot), reads are capped per query: `20`, then `10`, then `0`
