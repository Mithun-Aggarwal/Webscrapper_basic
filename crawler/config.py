"""Configuration loading utilities."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List
import yaml


def _default_exts() -> List[str]:
    return [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".zip"]


@dataclass
class Config:
    """Dataclass representing crawler configuration."""

    start_url: str
    allowed_domain: str
    output_dir: Path = Path("./out")
    state_dir: Path = Path("./state")
    max_pages: int = 10_000
    max_depth: int = 10
    respect_robots_txt: bool = True
    allowed_extensions: List[str] = field(default_factory=_default_exts)
    follow_subdomains: bool = False
    rate_limit_sec: float = 0.5
    timeout_sec: int = 20
    user_agent: str = "SimpleCrawler/0.1 (+https://example.com)"
    retries: int = 2
    ignore_query_params: bool = True


def load_config(path: Path) -> Config:
    """Load configuration from YAML file.

    Parameters
    ----------
    path: Path
        Path to YAML configuration file.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    cfg = Config(**data)
    cfg.output_dir = Path(cfg.output_dir)
    cfg.state_dir = Path(cfg.state_dir)
    return cfg
