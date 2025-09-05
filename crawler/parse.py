"""HTML parsing helpers."""
from __future__ import annotations

from typing import Iterable, List
from urllib.parse import urljoin
from bs4 import BeautifulSoup


def extract_links(html: str, base_url: str) -> List[str]:
    """Extract absolute links from HTML text."""
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        links.append(urljoin(base_url, a["href"]))
    return links
