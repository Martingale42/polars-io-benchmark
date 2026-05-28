from __future__ import annotations

import gc
import json
import pickle
import random
import statistics as stats
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl

DEFAULT_SEED = 42
DEFAULT_REPEATS = 30
DEFAULT_OUTDIR = Path("benchmark_out")
DEFAULT_SIZES = (1_000, 10_000, 100_000, 1_000_000)
FULL_SIZES = (
    *range(1_000, 10_001, 1_000),
    *range(20_000, 100_001, 10_000),
    *range(200_000, 1_000_001, 100_000),
    *range(2_000_000, 5_000_001, 1_000_000),
)

COLS = tuple(f"col_{i}" for i in range(1, 21))
CAT_COLS = tuple(f"col_{i}" for i in range(1, 5))
INT_COLS = tuple(f"col_{i}" for i in range(5, 9))
FLOAT_COLS = tuple(f"col_{i}" for i in range(9, 13))
BOOL_COLS = tuple(f"col_{i}" for i in range(13, 17))
DATE_COLS = tuple(f"col_{i}" for i in range(17, 21))

NONOPT_SCHEMA: dict[str, pl.DataType] = {
    **{c: pl.String for c in (*CAT_COLS, *INT_COLS, *FLOAT_COLS, *BOOL_COLS)},
    **{c: pl.Datetime("ns") for c in DATE_COLS},
}

OPT_SCHEMA: dict[str, pl.DataType] = {
    **{c: pl.Categorical for c in CAT_COLS},
    "col_5": pl.Int8,
    "col_6": pl.Int16,
    "col_7": pl.Int16,
    "col_8": pl.Int32,
    **{c: pl.Float32 for c in FLOAT_COLS},
    **{c: pl.Boolean for c in BOOL_COLS},
    **{c: pl.Datetime("ns") for c in DATE_COLS},
}


@dataclass(frozen=True)
class FormatSpec:
    suffix: str
    reader: Callable[[Path, dict[str, pl.DataType]], pl.DataFrame]
    writer: Callable[[pl.DataFrame, Path], None]


def make_base_df(n: int, seed: int = DEFAULT_SEED) -> pl.DataFrame:
    rng = np.random.default_rng(seed + n)
    categories = np.array(
        ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"],
        dtype=object,
    )

    data: dict[str, object] = {}
    for col in CAT_COLS:
        data[col] = rng.choice(categories, size=n)

    data["col_5"] = rng.integers(-100, 101, size=n, dtype=np.int16)
    data["col_6"] = rng.integers(-20_000, 20_001, size=n, dtype=np.int32)
    data["col_7"] = rng.integers(0, 30_001, size=n, dtype=np.int32)
    data["col_8"] = rng.integers(-1_000_000, 1_000_001, size=n, dtype=np.int64)

    for col in FLOAT_COLS:
        data[col] = rng.normal(loc=0.0, scale=1.0, size=n).astype(np.float64)

    for col in BOOL_COLS:
        data[col] = rng.choice([False, True], size=n)

    start = np.datetime64("2020-01-01T00:00:00", "ns")
    max_seconds = 5 * 365 * 24 * 60 * 60
    for col in DATE_COLS:
        offsets = rng.integers(0, max_seconds, size=n, dtype=np.int64)
        data[col] = start + offsets.astype("timedelta64[s]")

    return pl.DataFrame(data).select(COLS)


def cast_schema(df: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return df.with_columns(
        pl.col(col).cast(dtype) for col, dtype in schema.items()
    ).select(COLS).rechunk()


def make_nonopt_df(n: int, seed: int = DEFAULT_SEED) -> pl.DataFrame:
    return cast_schema(make_base_df(n, seed), NONOPT_SCHEMA)


def make_opt_df(n: int, seed: int = DEFAULT_SEED) -> pl.DataFrame:
    return cast_schema(make_base_df(n, seed), OPT_SCHEMA)


def csv_parse_schema(schema: dict[str, pl.DataType]) -> dict[str, pl.DataType]:
    csv_schema = dict(schema)
    for col in CAT_COLS:
        if csv_schema[col] == pl.Categorical:
            csv_schema[col] = pl.String
    return csv_schema


def read_csv_raw(path: Path, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.read_csv(path, schema=csv_parse_schema(schema), try_parse_dates=True)


def write_csv(df: pl.DataFrame, path: Path) -> None:
    df.write_csv(path)


def read_ipc_raw(path: Path, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    # memory_map=False forces eager read so IPC timing is comparable to
    # CSV/Parquet/Pickle which materialize data on read. Without this Polars
    # mmaps the file and `read` returns in ~1 ms regardless of file size,
    # making IPC look 1000x faster than reality and breaking the additivity
    # of read+write vs write.
    return pl.read_ipc(path, memory_map=False)


def write_ipc(df: pl.DataFrame, path: Path) -> None:
    df.write_ipc(path)


def read_parquet_raw(path: Path, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.read_parquet(path)


def write_parquet_snappy(df: pl.DataFrame, path: Path) -> None:
    df.write_parquet(path, compression="snappy")


def write_parquet_zstd(df: pl.DataFrame, path: Path) -> None:
    df.write_parquet(path, compression="zstd", compression_level=3)


def read_pickle_raw(path: Path, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    with path.open("rb") as file:
        return pickle.load(file)


def write_pickle(df: pl.DataFrame, path: Path) -> None:
    with path.open("wb") as file:
        pickle.dump(df, file, protocol=pickle.HIGHEST_PROTOCOL)


FORMATS: dict[str, FormatSpec] = {
    "csv": FormatSpec(".csv", read_csv_raw, write_csv),
    "feather_ipc": FormatSpec(".feather", read_ipc_raw, write_ipc),
    "parquet_snappy": FormatSpec(".parquet", read_parquet_raw, write_parquet_snappy),
    "parquet_zstd": FormatSpec(".parquet", read_parquet_raw, write_parquet_zstd),
    "pickle": FormatSpec(".pkl", read_pickle_raw, write_pickle),
}


def timed_once(fn: Callable[[], object]) -> float:
    """
    Definition: Run fn once with GC disabled, return wall-clock seconds.
    Domain:     fn is any side-effecting callable; raises propagate.
    Returns:    Elapsed seconds as float (perf_counter resolution).
    """
    gc.collect()
    gc.disable()
    try:
        start = time.perf_counter()
        fn()
        return time.perf_counter() - start
    finally:
        gc.enable()


def collect_stats(samples: list[float]) -> dict[str, object]:
    return {
        "min_s": min(samples),
        "median_s": stats.median(samples),
        "mean_s": stats.mean(samples),
        "samples_s": json.dumps(samples),
    }


def file_size_mb(path: Path) -> float:
    return path.stat().st_size / 1024 / 1024


# Shuffle order is deterministic per (n, kind) but independent across them,
# so any system-noise event is spread across formats/ops instead of hitting
# one column of the result table.
KIND_OFFSETS = {"nonopt": 0, "opt": 1}


def benchmark_one(
    n: int,
    kind: str,
    df: pl.DataFrame,
    schema: dict[str, pl.DataType],
    outdir: Path,
    repeats: int,
    formats: Sequence[str],
    seed: int,
) -> list[dict[str, object]]:
    ram_mb = df.estimated_size("mb")
    paths: dict[str, Path] = {}
    roundtrip_paths: dict[str, Path] = {}
    samples: dict[str, dict[str, list[float]]] = {
        fmt: {"write": [], "read": [], "rw": []} for fmt in formats
    }

    # Priming write: ensure files exist before any timed read.
    # This write is intentionally untimed.
    for fmt in formats:
        io = FORMATS[fmt]
        paths[fmt] = outdir / f"{kind}_{n}_{fmt}{io.suffix}"
        roundtrip_paths[fmt] = outdir / f"{kind}_{n}_{fmt}_roundtrip{io.suffix}"
        io.writer(df, paths[fmt])

    rng_local = random.Random(seed * 1_000_003 + n * 7 + KIND_OFFSETS[kind])

    for _ in range(repeats):
        ops = [(fmt, op) for fmt in formats for op in ("write", "read", "rw")]
        rng_local.shuffle(ops)
        for fmt, op in ops:
            io = FORMATS[fmt]
            path = paths[fmt]
            if op == "write":
                elapsed = timed_once(
                    lambda io=io, df=df, path=path: io.writer(df, path)
                )
            elif op == "read":
                elapsed = timed_once(
                    lambda io=io, path=path, schema=schema: io.reader(path, schema)
                )
            else:
                rpath = roundtrip_paths[fmt]

                def rw(io=io, path=path, rpath=rpath, schema=schema) -> None:
                    loaded = io.reader(path, schema)
                    io.writer(loaded, rpath)

                elapsed = timed_once(rw)
                rpath.unlink(missing_ok=True)
            samples[fmt][op].append(elapsed)

    rows: list[dict[str, object]] = []
    for fmt in formats:
        size_mb = file_size_mb(paths[fmt])
        w = collect_stats(samples[fmt]["write"])
        r = collect_stats(samples[fmt]["read"])
        rw_stats = collect_stats(samples[fmt]["rw"])
        rows.append(
            {
                "n": n,
                "kind": kind,
                "format": fmt,
                "ram_mb": ram_mb,
                "file_mb": size_mb,
                "write_min_s": w["min_s"],
                "write_median_s": w["median_s"],
                "write_mean_s": w["mean_s"],
                "write_samples_s": w["samples_s"],
                "read_min_s": r["min_s"],
                "read_median_s": r["median_s"],
                "read_mean_s": r["mean_s"],
                "read_samples_s": r["samples_s"],
                "read_write_min_s": rw_stats["min_s"],
                "read_write_median_s": rw_stats["median_s"],
                "read_write_mean_s": rw_stats["mean_s"],
                "read_write_samples_s": rw_stats["samples_s"],
            }
        )

    return rows


def run_benchmark(
    *,
    sizes: Iterable[int] = DEFAULT_SIZES,
    outdir: Path = DEFAULT_OUTDIR,
    repeats: int = DEFAULT_REPEATS,
    seed: int = DEFAULT_SEED,
    formats: Sequence[str] = tuple(FORMATS),
) -> pl.DataFrame:
    outdir.mkdir(parents=True, exist_ok=True)
    all_rows: list[dict[str, object]] = []

    for n in sizes:
        for kind, mk_df, schema in (
            ("nonopt", make_nonopt_df, NONOPT_SCHEMA),
            ("opt", make_opt_df, OPT_SCHEMA),
        ):
            print(f"Running n={n:,} {kind} ({repeats} reps, interleaved)")
            df = mk_df(n, seed)
            all_rows.extend(
                benchmark_one(n, kind, df, schema, outdir, repeats, list(formats), seed)
            )
            del df
            gc.collect()

    results = pl.DataFrame(all_rows).sort(["n", "kind", "format"])
    results.write_csv(outdir / "polars_io_benchmark_results.csv")
    return results
