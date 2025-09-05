"""Command line interface for the crawler."""
from __future__ import annotations

import argparse
from pathlib import Path

from .config import load_config
from .crawl import discover
from .download import download as download_files


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="crawler")
    parser.add_argument("mode", choices=["discover", "download"], help="Operation mode")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args(argv)

    cfg = load_config(Path(args.config))
    if args.mode == "discover":
        pages, ext_counts = discover(cfg)
        total_files = sum(ext_counts.values())
        print(f"Visited {pages} pages; discovered {total_files} files: {dict(ext_counts)}")
    else:
        downloaded, skipped, failed = download_files(cfg)
        print(
            f"Downloaded {downloaded} files; skipped {skipped} unchanged; {failed} failures"
        )


if __name__ == "__main__":  # pragma: no cover
    main()
