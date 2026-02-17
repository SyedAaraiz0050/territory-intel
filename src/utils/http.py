from __future__ import annotations

import requests
from typing import Any, Dict, Optional
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type


class HttpError(RuntimeError):
    pass


@retry(
    wait=wait_exponential(min=1, max=20),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((requests.RequestException, HttpError)),
    reraise=True,
)
def get_json(
    url: str,
    *,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    json: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    resp = requests.request(
        method=method.upper().strip(),
        url=url,
        params=params,
        headers=headers,
        json=json,
        timeout=timeout,
    )

    if not resp.ok:
        raise HttpError(f"HTTP {resp.status_code}: {resp.text[:300]}")

    return resp.json()