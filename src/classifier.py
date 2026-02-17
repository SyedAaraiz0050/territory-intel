# src/classifier.py
from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional

import requests
from pydantic import BaseModel, Field, conint, ValidationError

from src.config import get_settings

OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"


class Classification(BaseModel):
    industry_bucket: str

    mobility_fit: conint(ge=0, le=100)
    security_fit: conint(ge=0, le=100)
    voip_fit: conint(ge=0, le=100)
    fleet_attach: conint(ge=0, le=100)

    signal_after_hours: conint(ge=0, le=1)
    signal_dispatch: conint(ge=0, le=1)
    signal_field_work: conint(ge=0, le=1)

    ai_reason: str = Field(..., max_length=400)


# -----------------------------
# Homepage fetch (single page)
# -----------------------------
def _html_to_text(html: str) -> str:
    html = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style.*?>.*?</style>", " ", html)
    text = re.sub(r"(?s)<.*?>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def fetch_homepage_text(website_url: str, *, timeout: int = 20, max_chars: int = 10_000) -> str:
    headers = {"User-Agent": "territory-intel/1.0", "Accept": "text/html,application/xhtml+xml"}
    r = requests.get(website_url, headers=headers, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    text = _html_to_text(r.text)
    return text[:max_chars]


# -----------------------------
# Response parsing + repair
# -----------------------------
def _extract_output_text(resp_json: Dict[str, Any]) -> str:
    parts: list[str] = []
    for item in resp_json.get("output", []) or []:
        for c in item.get("content", []) or []:
            if c.get("type") == "output_text" and isinstance(c.get("text"), str):
                t = c["text"].strip()
                if t:
                    parts.append(t)
    return "\n".join(parts).strip()


def _strip_fences(s: str) -> str:
    s = s.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z]*\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
    return s.strip()


def _extract_first_json_object(s: str) -> str:
    """
    If model returns extra text, pull the first {...} JSON object.
    """
    s = _strip_fences(s)
    m = re.search(r"\{.*\}", s, flags=re.DOTALL)
    if not m:
        return s
    return m.group(0).strip()


def _to_int(x: Any, *, lo: int, hi: int) -> int:
    """
    Convert strings/floats/bools to int and clamp.
    """
    if isinstance(x, bool):
        v = 1 if x else 0
    elif isinstance(x, (int, float)):
        v = int(round(float(x)))
    elif isinstance(x, str):
        # extract first number in string (handles "85%" or "85/100")
        m = re.search(r"-?\d+(\.\d+)?", x)
        v = int(round(float(m.group(0)))) if m else lo
    else:
        v = lo

    if v < lo:
        v = lo
    if v > hi:
        v = hi
    return v


def _normalize(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize to match Classification schema.
    """
    out: Dict[str, Any] = {}

    out["industry_bucket"] = str(obj.get("industry_bucket") or "Unknown").strip()[:80]

    out["mobility_fit"] = _to_int(obj.get("mobility_fit"), lo=0, hi=100)
    out["security_fit"] = _to_int(obj.get("security_fit"), lo=0, hi=100)
    out["voip_fit"] = _to_int(obj.get("voip_fit"), lo=0, hi=100)
    out["fleet_attach"] = _to_int(obj.get("fleet_attach"), lo=0, hi=100)

    out["signal_after_hours"] = _to_int(obj.get("signal_after_hours"), lo=0, hi=1)
    out["signal_dispatch"] = _to_int(obj.get("signal_dispatch"), lo=0, hi=1)
    out["signal_field_work"] = _to_int(obj.get("signal_field_work"), lo=0, hi=1)

    reason = obj.get("ai_reason")
    if reason is None:
        reason = "No reason provided."
    out["ai_reason"] = str(reason).strip()[:400]

    return out


# -----------------------------
# Public API
# -----------------------------
def classify_business(
    *,
    name: str,
    address: str,
    primary_type: Optional[str],
    website: Optional[str],
    homepage_text: Optional[str],
    model: str = "gpt-4.1-mini",
    max_output_tokens: int = 250,
) -> Classification:
    s = get_settings()
    api_key = s.openai_api_key
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY missing in .env")

    info = {
        "name": name,
        "address": address,
        "primary_type": primary_type,
        "website": website,
        "homepage_text": homepage_text,
    }

    prompt = (
        "Return ONLY valid JSON. No markdown. No extra text.\n"
        "Keys required:\n"
        "industry_bucket, mobility_fit, security_fit, voip_fit, fleet_attach,\n"
        "signal_after_hours, signal_dispatch, signal_field_work, ai_reason.\n"
        "Rules:\n"
        "- fits are integers 0-100\n"
        "- signals are integers 0 or 1\n"
        "- ai_reason <= 400 chars\n"
        "- Mobility is highest priority; Security then VoIP then Fleet.\n\n"
        f"Business:\n{json.dumps(info, ensure_ascii=False)}"
    )

    payload: Dict[str, Any] = {
        "model": model,
        "input": prompt,
        "max_output_tokens": max_output_tokens,
        "temperature": 0.2,
    }

    r = requests.post(
        OPENAI_RESPONSES_URL,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=60,
    )

    if r.status_code != 200:
        raise RuntimeError(f"OpenAI error {r.status_code}: {r.text[:1200]}")

    raw_text = _extract_output_text(r.json())
    raw_text = _extract_first_json_object(raw_text)

    # 1) try strict
    try:
        return Classification.model_validate_json(raw_text)
    except ValidationError:
        # 2) repair path: parse -> normalize -> validate
        try:
            parsed = json.loads(raw_text)
        except Exception:
            # If JSON parse fails, hard fail with useful snippet
            raise RuntimeError(f"Classifier output not parseable as JSON: {raw_text[:500]}")

        normalized = _normalize(parsed)
        return Classification.model_validate(normalized)