"""State persistence utilities."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Set, Any


@dataclass
class CrawlState:
    """Represents crawling state loaded from disk."""

    state_dir: Path
    visited: Set[str] = field(default_factory=set)
    manifest: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.visited_file = self.state_dir / "visited_urls.txt"
        self.manifest_file = self.state_dir / "manifest.jsonl"
        self._load()

    def _load(self) -> None:
        if self.visited_file.exists():
            self.visited = set(
                url.strip() for url in self.visited_file.read_text().splitlines() if url.strip()
            )
        if self.manifest_file.exists():
            with self.manifest_file.open("r", encoding="utf-8") as f:
                for line in f:
                    data = json.loads(line)
                    self.manifest[data["file_url"]] = data

    def save(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        with self.visited_file.open("w", encoding="utf-8") as f:
            for url in sorted(self.visited):
                f.write(url + "\n")
        with self.manifest_file.open("w", encoding="utf-8") as f:
            for entry in self.manifest.values():
                json.dump(entry, f)
                f.write("\n")
