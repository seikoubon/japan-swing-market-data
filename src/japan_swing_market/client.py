import os
import time
from urllib.parse import urlparse

import httpx


class APIError(RuntimeError):
    """Sanitized error: never includes API key, body, or signed URL."""


class Client:
    BASE = "https://api.jquants.com/v2"

    def __init__(self, key=None, transport=None, interval=1.05, sleep=time.sleep):
        self.key = key if key is not None else os.environ.get("JQUANTS_API_KEY", "")
        if not self.key:
            raise APIError("JQUANTS_API_KEY is not configured")
        self.http = httpx.Client(timeout=90, transport=transport)
        # Separate client without x-api-key for presigned download URLs.
        self.download_http = httpx.Client(timeout=180, follow_redirects=False, transport=transport)
        self.interval, self.sleep, self.last = interval, sleep, 0.0

    def close(self):
        self.http.close()
        self.download_http.close()

    def get(self, endpoint, params=None):
        if not endpoint.startswith("/") or endpoint.startswith("//") or "?" in endpoint:
            raise ValueError("Invalid endpoint")
        for attempt in range(5):
            self.sleep(max(0, self.interval - (time.monotonic() - self.last)))
            self.last = time.monotonic()
            try:
                r = self.http.get(self.BASE + endpoint, params=params, headers={"x-api-key": self.key})
            except httpx.TransportError:
                if attempt == 4:
                    raise APIError(f"Transport error at {endpoint}") from None
                self.sleep(2**attempt)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                if attempt == 4:
                    raise APIError(f"HTTP {r.status_code} at {endpoint}; retries exhausted")
                # J-Quants 429 has no Retry-After; allow full sliding window to pass.
                self.sleep(65 if r.status_code == 429 else 2**attempt)
                continue
            if r.status_code != 200:
                raise APIError(f"HTTP {r.status_code} at {endpoint}")
            try:
                body = r.json()
            except ValueError:
                raise APIError(f"Invalid JSON at {endpoint}") from None
            if not isinstance(body, dict):
                raise APIError(f"Unexpected response at {endpoint}")
            return body
        raise APIError("Unreachable retry state")

    def pages(self, endpoint, params=None):
        query, seen = dict(params or {}), set()
        while True:
            body = self.get(endpoint, query)
            if not isinstance(body.get("data"), list):
                raise APIError(f"Missing data array at {endpoint}")
            yield body
            token = body.get("pagination_key")
            if not token:
                break
            if token in seen:
                raise APIError(f"Repeated pagination token at {endpoint}")
            seen.add(token)
            query["pagination_key"] = token

    def download(self, key):
        for attempt in range(5):
            body = self.get("/bulk/get", {"key": key})
            url = body.get("url")
            if not isinstance(url, str) or urlparse(url).scheme != "https":
                raise APIError("Invalid bulk download URL")
            try:
                r = self.download_http.get(url)
                if r.status_code == 200:
                    return r.content
                if r.status_code not in {403, 429} and r.status_code < 500:
                    raise APIError(f"Bulk download HTTP {r.status_code}")
            except httpx.TransportError:
                pass
            if attempt == 4:
                raise APIError("Bulk download failed after retries") from None
            self.sleep(2**attempt)
        raise APIError("Unreachable bulk retry state")
