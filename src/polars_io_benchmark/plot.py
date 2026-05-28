from __future__ import annotations

import argparse
import os
from pathlib import Path

import polars as pl


DEFAULT_METRICS = (
    "read_median_s",
    "write_median_s",
    "read_write_median_s",
    "file_mb",
    "ram_mb",
)

METRIC_LABELS = {
    "read_median_s": ("Performance of loading", "Seconds"),
    "write_median_s": ("Performance of saving", "Seconds"),
    "read_write_median_s": ("Performance of loading+saving", "Seconds"),
    "file_mb": ("Performance of HDD usage", "MB"),
    "ram_mb": ("Performance of RAM usage", "MB"),
}

FORMAT_LABELS = {
    "csv": "CSV",
    "feather_ipc": "Feather",
    "parquet_snappy": "Parquet (snappy)",
    "parquet_zstd": "Parquet (zstd)",
    "pickle": "Pickle",
}

FORMAT_COLORS = {
    "csv": "#4c5fb3",
    "feather_ipc": "#c95f53",
    "parquet_snappy": "#2bb39c",
    "parquet_zstd": "#1b8f7a",
    "pickle": "#9460b8",
}

FORMAT_LINESTYLES = {
    "csv": "-",
    "feather_ipc": "-",
    "parquet_snappy": "-",
    "parquet_zstd": "--",
    "pickle": "-",
}


def format_record_count(value: float) -> str:
    if value == 0:
        return "0"
    millions = value / 1_000_000
    if millions.is_integer():
        return f"{int(millions)}M"
    return f"{millions:.1f}M"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Plot benchmark results. With no flags, reads "
            "benchmark_out/polars_io_benchmark_results.csv and writes all plots "
            "to benchmark_out/plots."
        )
    )
    parser.add_argument(
        "results_csv",
        nargs="?",
        type=Path,
        default=Path("benchmark_out/polars_io_benchmark_results.csv"),
        help="Benchmark results CSV. Default: benchmark_out/polars_io_benchmark_results.csv.",
    )
    parser.add_argument(
        "--metric",
        nargs="+",
        choices=DEFAULT_METRICS,
        default=list(DEFAULT_METRICS),
        help="Metric or metrics to plot. Default: all metrics.",
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        default=Path("benchmark_out/plots"),
        help="Directory for PNG outputs. Default: benchmark_out/plots.",
    )
    parser.add_argument(
        "--combined",
        action="store_true",
        help="Also write combined non-optimized + optimized plots for each metric.",
    )
    return parser


def plot_results(
    results_csv: Path,
    outdir: Path,
    metrics: tuple[str, ...] | list[str] = DEFAULT_METRICS,
    combined: bool = False,
) -> list[Path]:
    mpl_config_dir = outdir / ".matplotlib"
    mpl_config_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(mpl_config_dir))

    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "axes.facecolor": "#e8eef7",
            "axes.edgecolor": "#e8eef7",
            "axes.grid": True,
            "axes.labelcolor": "#263238",
            "axes.titlesize": 11,
            "axes.titleweight": "normal",
            "figure.facecolor": "white",
            "font.size": 9,
            "grid.color": "white",
            "grid.linewidth": 0.8,
            "legend.frameon": False,
            "lines.linewidth": 1.35,
            "xtick.color": "#263238",
            "ytick.color": "#263238",
        }
    )

    results = pl.read_csv(results_csv)
    outputs: list[Path] = []

    for metric in metrics:
        for kind in ("nonopt", "opt"):
            fig, ax = plt.subplots(figsize=(10.0, 6.5))
            fig.subplots_adjust(left=0.075, right=0.985, bottom=0.105, top=0.9)
            plot_metric_lines(ax, results, metric, kind)
            output = outdir / f"{kind}_{metric}.png"
            fig.savefig(output, dpi=160, bbox_inches="tight", pad_inches=0.08)
            plt.close(fig)
            outputs.append(output)
            print(f"Saved {output}")

        if combined:
            fig, ax = plt.subplots(figsize=(10.0, 6.5))
            fig.subplots_adjust(left=0.08, right=0.78, bottom=0.105, top=0.9)
            plot_metric_lines(ax, results, metric, None)
            output = outdir / f"combined_{metric}.png"
            fig.savefig(output, dpi=160, bbox_inches="tight", pad_inches=0.08)
            plt.close(fig)
            outputs.append(output)
            print(f"Saved {output}")

    return outputs


def plot_metric_lines(
    ax,
    results: pl.DataFrame,
    metric: str,
    kind: str | None,
) -> None:
    from matplotlib.ticker import FuncFormatter, MultipleLocator

    if kind is None:
        sub = results
        title_kind = "non-optimized and optimized dataframes"
    else:
        sub = results.filter(pl.col("kind") == kind)
        title_kind = "non-optimized dataframe" if kind == "nonopt" else "optimized dataframe"

    for format_name in sorted(sub["format"].unique().to_list()):
        if kind is None:
            for schema_kind, alpha in (("nonopt", 0.4), ("opt", 1.0)):
                series = sub.filter(
                    (pl.col("format") == format_name) & (pl.col("kind") == schema_kind)
                ).sort("n")
                label = f"{FORMAT_LABELS[format_name]}: {'non-optimized' if schema_kind == 'nonopt' else 'optimized'}"
                ax.plot(
                    series["n"],
                    series[metric],
                    color=FORMAT_COLORS[format_name],
                    linestyle=FORMAT_LINESTYLES[format_name],
                    alpha=alpha,
                    label=label,
                )
        else:
            series = sub.filter(pl.col("format") == format_name).sort("n")
            ax.plot(
                series["n"],
                series[metric],
                color=FORMAT_COLORS[format_name],
                linestyle=FORMAT_LINESTYLES[format_name],
                label=FORMAT_LABELS[format_name],
            )

    title_prefix, ylabel = METRIC_LABELS[metric]
    ax.set_title(f"{title_prefix} {title_kind} by using different formats", loc="left", pad=34)
    ax.set_xlabel("Number of Records", labelpad=12)
    ax.set_ylabel(ylabel, labelpad=10)
    ax.set_xlim(0, 5_000_000)
    ax.xaxis.set_major_locator(MultipleLocator(500_000))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: format_record_count(value)))
    ax.tick_params(axis="both", which="both", length=0)
    for spine in ax.spines.values():
        spine.set_visible(False)

    if kind is None:
        ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), borderaxespad=0.0)
    else:
        ax.legend(
            loc="lower right",
            bbox_to_anchor=(1.0, 1.005),
            ncol=5,
            borderaxespad=0.0,
            handlelength=2.0,
            columnspacing=1.6,
        )


def main() -> None:
    args = build_parser().parse_args()
    plot_results(args.results_csv, args.outdir, args.metric, args.combined)


if __name__ == "__main__":
    main()
