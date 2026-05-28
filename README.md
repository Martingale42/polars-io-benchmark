# polars-io-benchmark

A slim `uv` project replicating the LinkedIn article's pandas DataFrame
I/O benchmark with Polars: CSV, Feather / Arrow IPC, Pickle, Parquet
(Snappy & Zstd), across 32 row counts (1k–5M) and both non-optimized
and optimized schemas.

Reports live in [`reports/`](reports/):

- [`reports/POLARS_REPLICATION_REPORT.md`](reports/POLARS_REPLICATION_REPORT.md)
  — Polars vs the original pandas study; per-scale rankings (small /
  medium / large / very large) and per-scenario format recommendations.
- [`reports/REVIEW_REPORT.md`](reports/REVIEW_REPORT.md) — code review
  of the first-round measurement pipeline, the four bugs it found
  (read timing contaminated by `cast_schema` + `rechunk`, 5-repeat
  instability, IPC `memory_map=True` illusion, GC inside the timed
  window), and verification of the fixes.

## Schema design

This is **not** a bit-for-bit reproduction of pandas `object` columns.
Polars `String` is the practical non-optimized baseline; `Object` is a
Python escape hatch and is not representative for columnar I/O.

| Column group | Non-optimized Polars schema | Optimized Polars schema |
| --- | --- | --- |
| `col_1` - `col_4` | `String` | `Categorical` |
| `col_5` | `String` | `Int8` |
| `col_6` - `col_7` | `String` | `Int16` |
| `col_8` | `String` | `Int32` |
| `col_9` - `col_12` | `String` | `Float32` |
| `col_13` - `col_16` | `String` | `Boolean` |
| `col_17` - `col_20` | `Datetime(ns)` | `Datetime(ns)` |

## Setup

```bash
uv sync
```

## Run a smoke benchmark

```bash
uv run polars-io-benchmark --sizes 1000 10000 --repeats 5
```

Results are written to:

```text
benchmark_out/polars_io_benchmark_results.csv
benchmark_out/plots/
```

Generated intermediate files (one per row-count × kind × format) also
land under `benchmark_out/` and are gitignored; only the plots are
committed so that the reports render on GitHub.

## Run the default benchmark

```bash
uv run polars-io-benchmark
```

Default row counts: `1_000`, `10_000`, `100_000`, `1_000_000`.
Default repeats: 30 (the second-round value; first-round used 5).

## Run the fuller sweep

```bash
uv run polars-io-benchmark --full
```

The fuller sweep covers 1k–5M rows (32 row counts) and writes all
plots to `benchmark_out/plots`. On the reference machine (Ryzen 9 3900X)
this takes 90–120 minutes with the default `--repeats 30`.

To run the benchmark without generating plots:

```bash
uv run polars-io-benchmark --full --no-plot
```

To run the full sweep and also write combined (nonopt + opt) plots:

```bash
uv run polars-io-benchmark --full --plot-combined
```

## Measurement methodology

Each `(n, kind, format)` cell records 30 raw timing samples per metric
(write, read, read+write). Within each `(n, kind)` block the 15
`(format, op)` pairs are reshuffled per repeat so that transient
system noise spreads across cells instead of concentrating on one
column. Garbage collection is forced before each measurement and
disabled inside the timing window. The result CSV preserves the 30
raw samples per metric in JSON-encoded `*_samples_s` columns so any
statistic can be re-derived without re-running.

`pl.read_ipc` is called with `memory_map=False`, otherwise the read
returns in ~1 ms regardless of file size and read+write timing breaks
additivity vs write. `cast_schema` and `rechunk` run outside the
timing window so the reader timing reflects raw I/O only.

See `reports/REVIEW_REPORT.md` §10 for the full list of measurement
bugs that motivated this methodology.

## Select formats

```bash
uv run polars-io-benchmark \
  --sizes 1000 10000 \
  --formats csv feather_ipc parquet_snappy parquet_zstd
```

Available formats:

- `csv`
- `feather_ipc`
- `parquet_snappy`
- `parquet_zstd` (zstd compression level pinned at 3)
- `pickle` (Polars `DataFrame.__reduce__` writes Arrow IPC under the
  hood, so `pickle` and `feather_ipc` produce nearly identical bytes;
  see `reports/POLARS_REPLICATION_REPORT.md` §9)

## Plot results

```bash
uv run polars-io-plot
```

Reads `benchmark_out/polars_io_benchmark_results.csv` and writes all
plots to `benchmark_out/plots`.

Plot a single metric:

```bash
uv run polars-io-plot --metric read_median_s
```

Combined non-optimized + optimized plots:

```bash
uv run polars-io-plot --combined
```

The benchmark command supports the same combined plot mode:

```bash
uv run polars-io-benchmark --full --plot-combined
```

Supported metrics:

- `read_median_s`
- `write_median_s`
- `read_write_median_s`
- `file_mb`
- `ram_mb`

## Output columns

The result CSV contains one row per row-count × schema kind × format:

- `n`, `kind` (`nonopt` / `opt`), `format`
- `ram_mb`, `file_mb`
- `write_min_s`, `write_median_s`, `write_mean_s`, `write_samples_s`
- `read_min_s`, `read_median_s`, `read_mean_s`, `read_samples_s`
- `read_write_min_s`, `read_write_median_s`, `read_write_mean_s`,
  `read_write_samples_s`

The `*_samples_s` columns contain the 30 raw timings as JSON arrays so
the reports' rank and tie claims can be re-verified.
