"""Streamlit dashboard for viewing crawl state."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

import streamlit as st

from crawler.config import load_config
from crawler.state import CrawlState


def _load_entries(state_dir: Path) -> List[Dict[str, Any]]:
    """Load manifest entries from a state directory."""
    if not state_dir.exists():
        return []
    state = CrawlState(state_dir)
    return list(state.manifest.values())


def _get_state_dir(config_path: str) -> Path:
    """Determine state directory from optional config file path."""
    if config_path:
        try:
            cfg = load_config(Path(config_path))
            return Path(cfg.state_dir)
        except Exception:
            pass
    return Path("state")


def main() -> None:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--config", default="", help="Path to config file")
    args, _ = parser.parse_known_args()

    config_path = st.text_input("Config file path", value=args.config)
    state_dir = _get_state_dir(config_path)
    entries = _load_entries(state_dir)

    st.write(f"Loaded {len(entries)} manifest entries from {state_dir}")

    counts = Counter(e.get("status", "unknown") for e in entries)
    if counts:
        st.table([{ "status": k, "count": v } for k, v in counts.items()])
    else:
        st.write("No manifest entries found.")

    query = st.text_input("Search manifest", "").lower()
    if query:
        filtered = [e for e in entries if query in json.dumps(e).lower()]
    else:
        filtered = entries

    if filtered:
        st.dataframe(filtered)
    else:
        st.write("No results for search query.")


if __name__ == "__main__":
    main()
