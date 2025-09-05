"""Miscellaneous utility helpers."""
from __future__ import annotations

import time
import hashlib
from pathlib import Path
from urllib.parse import urlparse, urlunparse, urljoin
import requests


def normalize_url(url: str, ignore_query: bool = True) -> str:
    """Normalize URL by stripping fragment and optionally query parameters."""
    parsed = urlparse(url)
    query = "" if ignore_query else parsed.query
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path or "/", "", query, ""))


def is_within_domain(url: str, allowed_domain: str, follow_subdomains: bool) -> bool:
    host = urlparse(url).hostname or ""
    if follow_subdomains:
        return host.endswith(allowed_domain)
    return host == allowed_domain


def file_url_to_path(url: str) -> Path:
    """Convert a file URL to a relative local path."""
    parsed = urlparse(url)
    return Path(parsed.path.lstrip("/"))


def request_with_retries(session: requests.Session, method: str, url: str, retries: int, timeout: int, **kwargs) -> requests.Response:
    """Perform HTTP request with simple retry logic."""
    for attempt in range(retries + 1):
        try:
            resp = session.request(method, url, timeout=timeout, **kwargs)
            return resp
        except requests.RequestException:
            if attempt == retries:
                raise
            time.sleep(0.5)


def sha256_file(path: Path) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
