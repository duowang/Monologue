"""A fake requests.Session that serves canned responses, so crawlers can be tested offline."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field

import pytest
import requests


@dataclass
class FakeResponse:
    status_code: int = 200
    text: str = ""
    headers: dict[str, str] = field(default_factory=dict)
    url: str = ""

    def json(self):
        return json.loads(self.text)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}", response=self)


class FakeSession:
    """Routes GETs to a handler(url, params) -> FakeResponse | int (status) | Exception."""

    def __init__(self, handler: Callable[[str, dict], object]):
        self.handler = handler
        self.calls: list[tuple[str, dict]] = []
        self.headers: dict[str, str] = {}

    def get(self, url, params=None, timeout=None):
        params = dict(params or {})
        self.calls.append((url, params))
        result = self.handler(url, params)
        if isinstance(result, Exception):
            raise result
        if isinstance(result, int):
            return FakeResponse(status_code=result, url=url)
        return result


def wp_posts(posts: list[dict], total_pages: int = 1) -> FakeResponse:
    return FakeResponse(text=json.dumps(posts), headers={"X-WP-TotalPages": str(total_pages)})


def wp_post(date: str, html: str, *, title: str = "Transcript", link: str = "https://x/transcript") -> dict:
    return {
        "id": 1,
        "date": f"{date}T01:00:00",
        "link": link,
        "title": {"rendered": title},
        "content": {"rendered": html},
    }


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _s: None)
