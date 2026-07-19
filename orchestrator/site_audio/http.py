from __future__ import annotations

import time
from typing import Callable, Mapping

from .errors import DownloadFailure
from .hls import TextResponse


_TRANSIENT_STATUSES = {429, 500, 502, 503, 504}


class CurlCffiTransport:
    def __init__(
        self,
        *,
        session=None,
        timeout: float = 30.0,
        max_attempts: int = 3,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if session is None:
            from curl_cffi import requests as curl_requests

            session = curl_requests.Session(impersonate="chrome")
        self.session = session
        self.timeout = timeout
        self.max_attempts = max(1, max_attempts)
        self.sleep = sleep

    def get_text(self, url: str, headers: Mapping[str, str]) -> TextResponse:
        response = None
        for attempt in range(self.max_attempts):
            try:
                response = self.session.get(
                    url,
                    headers=dict(headers),
                    timeout=self.timeout,
                    allow_redirects=True,
                )
            except Exception as exc:
                if attempt == self.max_attempts - 1:
                    raise DownloadFailure("HTTP request failed", retryable=True) from exc
                self.sleep(float(2**attempt))
                continue
            if response.status_code not in _TRANSIENT_STATUSES or attempt == self.max_attempts - 1:
                break
            self.sleep(float(2**attempt))
        assert response is not None
        return TextResponse(url=str(response.url), status_code=response.status_code, text=response.text)
