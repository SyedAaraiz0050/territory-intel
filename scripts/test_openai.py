# scripts/test_openai.py
from __future__ import annotations

import requests
from src.config import get_settings

RESPONSES_URL = "https://api.openai.com/v1/responses"


def main() -> None:
    s = get_settings()
    api_key = s.openai_api_key
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not loaded from .env")

    payload = {
        "model": "gpt-4.1-mini",
        "input": "Reply with exactly: OK",
        "max_output_tokens": 16,  # minimum allowed
    }

    r = requests.post(
        RESPONSES_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=60,
    )

    print("Status:", r.status_code)
    print(r.text[:500])


if __name__ == "__main__":
    main()