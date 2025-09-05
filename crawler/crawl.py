"""Crawling logic for discovery mode."""
from __future__ import annotations

from collections import deque, defaultdict
from datetime import datetime
import logging
import time
from pathlib import Path
from typing import Dict, Set, Deque, Tuple
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests

from .config import Config
from .state import CrawlState
from .parse import extract_links
from .utils import normalize_url, is_within_domain, request_with_retries


logger = logging.getLogger(__name__)


def discover(cfg: Config) -> Tuple[int, Dict[str, int]]:
    """Perform crawl and log discovered files.

    Returns
    -------
    tuple
        pages_visited, dict of extension -> count
    """
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    handler = logging.FileHandler(logs_dir / "crawl.log")
    logging.basicConfig(level=logging.INFO, handlers=[handler])

    state = CrawlState(cfg.state_dir)
    session = requests.Session()
    session.headers["User-Agent"] = cfg.user_agent

    rp: RobotFileParser | None = None
    if cfg.respect_robots_txt:
        rp = RobotFileParser()
        robots_url = f"https://{cfg.allowed_domain}/robots.txt"
        try:
            rp.set_url(robots_url)
            rp.read()
        except Exception:
            rp = None

    queue: Deque[Tuple[str, int]] = deque([(cfg.start_url, 0)])
    pages_visited = 0
    ext_counts: Dict[str, int] = defaultdict(int)

    while queue and pages_visited < cfg.max_pages:
        url, depth = queue.popleft()
        norm = normalize_url(url, cfg.ignore_query_params)
        if norm in state.visited or depth > cfg.max_depth:
            continue
        if not is_within_domain(norm, cfg.allowed_domain, cfg.follow_subdomains):
            continue
        if rp and not rp.can_fetch(cfg.user_agent, norm):
            logger.info("Blocked by robots: %s", norm)
            continue
        try:
            resp = request_with_retries(session, "GET", norm, cfg.retries, cfg.timeout_sec)
        except Exception as exc:
            logger.warning("Failed to fetch %s: %s", norm, exc)
            state.visited.add(norm)
            continue
        state.visited.add(norm)
        pages_visited += 1
        ctype = resp.headers.get("Content-Type", "")
        if "text/html" not in ctype:
            continue
        links = extract_links(resp.text, norm)
        for link in links:
            link_norm = normalize_url(link, cfg.ignore_query_params)
            if any(link_norm.lower().endswith(ext) for ext in cfg.allowed_extensions):
                ext = Path(link_norm).suffix.lower()
                ext_counts[ext] += 1
                entry = state.manifest.get(link_norm, {})
                entry.update(
                    {
                        "discovered_at": datetime.utcnow().isoformat(),
                        "source_page": norm,
                        "file_url": link_norm,
                        "file_path": None,
                        "status": "discovered",
                        "http_status": None,
                        "etag": None,
                        "last_modified": None,
                        "sha256": None,
                        "size_bytes": None,
                    }
                )
                state.manifest[link_norm] = entry
            else:
                if link_norm not in state.visited:
                    queue.append((link_norm, depth + 1))
        time.sleep(cfg.rate_limit_sec)
    state.save()
    return pages_visited, ext_counts
