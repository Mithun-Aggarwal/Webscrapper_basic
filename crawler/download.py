"""Download logic for files discovered by the crawler."""
from __future__ import annotations

import logging
import hashlib
from typing import Tuple

import requests
from tqdm import tqdm

from .config import Config
from .state import CrawlState
from .utils import (
    file_url_to_path,
    request_with_retries,
)

logger = logging.getLogger(__name__)


def download(cfg: Config) -> Tuple[int, int, int]:
    """Download files listed in the manifest.

    Returns
    -------
    tuple
        downloaded, skipped, failed counts
    """
    state = CrawlState(cfg.state_dir)
    session = requests.Session()
    session.headers["User-Agent"] = cfg.user_agent
    cfg.output_dir.mkdir(parents=True, exist_ok=True)

    downloaded = skipped = failed = 0
    with tqdm(total=len(state.manifest), desc="Downloading") as pbar:
        for url, entry in state.manifest.items():
            tqdm.write(f"Fetching {url}")
            try:
                head = request_with_retries(session, "HEAD", url, cfg.retries, cfg.timeout_sec, allow_redirects=True)
                etag = head.headers.get("ETag")
                last_mod = head.headers.get("Last-Modified")
                if (etag and entry.get("etag") == etag) or (
                    last_mod and entry.get("last_modified") == last_mod
                ):
                    entry.update({
                        "status": "skipped_unchanged",
                        "http_status": head.status_code,
                        "etag": etag,
                        "last_modified": last_mod,
                    })
                    skipped += 1
                    state.manifest[url] = entry
                    pbar.update(1)
                    continue
                resp = request_with_retries(session, "GET", url, cfg.retries, cfg.timeout_sec, stream=True)
                resp.raise_for_status()
                rel = file_url_to_path(url)
                dest = cfg.output_dir / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                h = hashlib.sha256()
                size = 0
                with open(dest, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if not chunk:
                            continue
                        f.write(chunk)
                        h.update(chunk)
                        size += len(chunk)
                entry.update({
                    "file_path": str(dest),
                    "status": "downloaded",
                    "http_status": resp.status_code,
                    "etag": etag,
                    "last_modified": last_mod,
                    "sha256": h.hexdigest(),
                    "size_bytes": size,
                })
                downloaded += 1
            except Exception as exc:
                logger.warning("Failed to download %s: %s", url, exc)
                entry.update({"status": "failed"})
                failed += 1
            state.manifest[url] = entry
            pbar.update(1)
    state.save()
    return downloaded, skipped, failed
