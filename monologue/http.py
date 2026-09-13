"""HTTP helpers: a session with an honest User-Agent, retries with backoff, WordPress paging."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator, Mapping
from datetime import date

import requests

from monologue import __version__

log = logging.getLogger(__name__)

DEFAULT_USER_AGENT = f"monologue-crawler/{__version__} (+https://github.com/duowang/Monologue)"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def make_session(user_agent: str | None = DEFAULT_USER_AGENT) -> requests.Session:
    """Create a session. Pass user_agent=None to keep the requests default."""
    session = requests.Session()
    if user_agent:
        session.headers["User-Agent"] = user_agent
    return session


def get(
    session: requests.Session,
    url: str,
    *,
    params: Mapping[str, object] | None = None,
    timeout: float = 30.0,
    retries: int = 3,
    backoff: float = 0.8,
) -> requests.Response | None:
    """GET with retries. Returns None on 404; raises the last error after `retries` attempts."""
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, params=params, timeout=timeout)
        except requests.RequestException as exc:
            last_error = exc
            log.debug("attempt %d/%d failed for %s: %s", attempt, retries, url, exc)
        else:
            if response.status_code == 404:
                return None
            if response.status_code not in RETRYABLE_STATUS:
                response.raise_for_status()
                return response
            last_error = requests.HTTPError(f"{response.status_code} for {url}", response=response)
            log.debug("attempt %d/%d got %s for %s", attempt, retries, response.status_code, url)
        if attempt < retries:
            time.sleep(backoff * attempt)
    assert last_error is not None
    raise last_error


def iter_wp_posts(
    session: requests.Session,
    api_url: str,
    *,
    tag_id: int,
    after: date | None = None,
    before: date | None = None,
    per_page: int = 100,
    timeout: float = 35.0,
) -> Iterator[dict]:
    """Yield posts with a tag from a WordPress REST API, newest first, filtered server-side."""
    params: dict[str, object] = {
        "tags": tag_id,
        "per_page": per_page,
        "orderby": "date",
        "order": "desc",
        "_fields": "id,date,link,title,content",
    }
    if after:
        params["after"] = f"{after.isoformat()}T00:00:00"
    if before:
        params["before"] = f"{before.isoformat()}T23:59:59"

    page = 1
    while True:
        response = get(session, api_url, params={**params, "page": page}, timeout=timeout, retries=4)
        if response is None:
            return
        posts = response.json()
        if not posts:
            return
        yield from posts
        total_pages = int(response.headers.get("X-WP-TotalPages", "1"))
        if page >= total_pages:
            return
        page += 1
