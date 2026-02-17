# src/google_places.py

"""
Google Places API (New) integration layer.

Purpose:
- Discovery: Accept a human-readable query (e.g. "plumber in St. John's Newfoundland")
  and return a list of PlaceLite (place_id + basic metadata).
- Enrichment: Given a place_id, fetch Place Details to get phone, website, hours, rating,
  review count, and Google Maps URL (call-ready fields).

Design:
- API-focused
- Database-agnostic
- Output-agnostic
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from src.config import get_settings
from src.utils.http import get_json


# -----------------------------
# Endpoints (Places API New)
# -----------------------------

# Text Search
PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

# Place Details (by place_id)
# GET https://places.googleapis.com/v1/places/{place_id}
PLACES_DETAILS_BASE_URL = "https://places.googleapis.com/v1/places/"


# -----------------------------
# Field Masks
# -----------------------------

# Text Search field mask (places.* + nextPageToken)
TEXT_SEARCH_FIELD_MASK = ",".join(
    [
        "places.id",
        "places.displayName",
        "places.formattedAddress",
        "places.location",
        "places.types",
        "places.primaryType",
        "places.businessStatus",
        "nextPageToken",
    ]
)

# Place Details field mask (single place object fields, NOT places.*)
DETAILS_FIELD_MASK = ",".join(
    [
        "id",
        "displayName",
        "formattedAddress",
        "location",
        "types",
        "primaryType",
        "businessStatus",
        "internationalPhoneNumber",
        "nationalPhoneNumber",
        "websiteUri",
        "rating",
        "userRatingCount",
        "googleMapsUri",
        "regularOpeningHours",
    ]
)

# -----------------------------
# NL bias (simple province bbox)
# -----------------------------

NL_LOCATION_BIAS = {
    "rectangle": {
        "low": {"latitude": 46.5, "longitude": -59.5},
        "high": {"latitude": 54.9, "longitude": -52.0},
    }
}


# -----------------------------
# Models
# -----------------------------

@dataclass(frozen=True)
class PlaceLite:
    """Lightweight result from discovery."""
    place_id: str
    name: str
    address: str
    lat: Optional[float]
    lng: Optional[float]
    primary_type: Optional[str]
    types: List[str]
    business_status: Optional[str]


@dataclass(frozen=True)
class PlaceDetails:
    """Enriched call-ready details for a place_id."""
    place_id: str
    name: str
    address: str
    phone: Optional[str]
    website: Optional[str]
    rating: Optional[float]
    review_count: Optional[int]
    maps_url: Optional[str]
    opening_hours_json: Optional[dict]

    lat: Optional[float]
    lng: Optional[float]
    primary_type: Optional[str]
    types: List[str]
    business_status: Optional[str]


# -----------------------------
# Header builders
# -----------------------------

def _text_headers(api_key: str) -> Dict[str, str]:
    return {
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": TEXT_SEARCH_FIELD_MASK,
        "Content-Type": "application/json; charset=utf-8",
    }


def _details_headers(api_key: str) -> Dict[str, str]:
    return {
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": DETAILS_FIELD_MASK,
    }


# -----------------------------
# Parsers
# -----------------------------

def _parse_place_lite(p: Dict[str, Any]) -> PlaceLite:
    place_id = p.get("id") or ""
    name = (p.get("displayName") or {}).get("text") or ""
    address = p.get("formattedAddress") or ""
    loc = p.get("location") or {}

    lat = loc.get("latitude")
    lng = loc.get("longitude")

    primary_type = p.get("primaryType")
    types = p.get("types") or []
    business_status = p.get("businessStatus")

    return PlaceLite(
        place_id=place_id,
        name=name,
        address=address,
        lat=float(lat) if lat is not None else None,
        lng=float(lng) if lng is not None else None,
        primary_type=primary_type,
        types=list(types),
        business_status=business_status,
    )


def _parse_place_details(d: Dict[str, Any]) -> PlaceDetails:
    place_id = d.get("id") or ""
    name = (d.get("displayName") or {}).get("text") or ""
    address = d.get("formattedAddress") or ""
    loc = d.get("location") or {}

    # Phone fallback logic
    phone = d.get("internationalPhoneNumber") or d.get("nationalPhoneNumber") or None

    rating = d.get("rating")
    review_count = d.get("userRatingCount")

    return PlaceDetails(
        place_id=place_id,
        name=name,
        address=address,
        phone=phone,
        website=d.get("websiteUri"),
        rating=float(rating) if rating is not None else None,
        review_count=int(review_count) if review_count is not None else None,
        maps_url=d.get("googleMapsUri"),
        opening_hours_json=d.get("regularOpeningHours"),
        lat=float(loc.get("latitude")) if loc.get("latitude") is not None else None,
        lng=float(loc.get("longitude")) if loc.get("longitude") is not None else None,
        primary_type=d.get("primaryType"),
        types=list(d.get("types") or []),
        business_status=d.get("businessStatus"),
    )


# -----------------------------
# Public API
# -----------------------------

def text_search(
    query: str,
    *,
    page_size: int = 20,
    max_pages: int = 3,
    region_code: str = "CA",
    language_code: str = "en",
    use_nl_bias: bool = True,
    # Optional knobs for later (safe defaults = None/False)
    included_type: Optional[str] = None,
    strict_type_filtering: bool = False,
    # Safety: pagination token can require a short delay before it becomes valid
    page_token_delay_seconds: float = 2.0,
) -> List[PlaceLite]:
    """
    Perform Places API (New) Text Search.

    Returns:
      List[PlaceLite]
    """
    s = get_settings()
    api_key = s.google_maps_api_key
    if not api_key:
        raise RuntimeError("GOOGLE_MAPS_API_KEY is missing in .env")

    headers = _text_headers(api_key)

    payload: Dict[str, Any] = {
        "textQuery": query,
        "pageSize": page_size,
        "regionCode": region_code,
        "languageCode": language_code,
    }

    if use_nl_bias:
        payload["locationBias"] = NL_LOCATION_BIAS

    # Optional type filter (use later if you want)
    if included_type:
        payload["includedType"] = included_type
        payload["strictTypeFiltering"] = bool(strict_type_filtering)

    results: List[PlaceLite] = []
    seen: set[str] = set()
    page_token: Optional[str] = None

    for _ in range(max_pages):
        if page_token:
            payload["pageToken"] = page_token
        else:
            payload.pop("pageToken", None)

        data = get_json(
            PLACES_TEXT_SEARCH_URL,
            method="POST",
            headers=headers,
            json=payload,
            timeout=30,
        )

        for p in data.get("places") or []:
            place = _parse_place_lite(p)
            if place.place_id and place.place_id not in seen:
                seen.add(place.place_id)
                results.append(place)

        page_token = data.get("nextPageToken")
        if not page_token:
            break

        # Safety: token often needs a short delay before it is accepted
        if page_token_delay_seconds and page_token_delay_seconds > 0:
            time.sleep(page_token_delay_seconds)

    return results


def get_place_details(place_id: str) -> PlaceDetails:
    """
    Fetch Place Details (New) for a single place_id.

    Returns:
      PlaceDetails with phone, website, hours, rating, etc.
    """
    s = get_settings()
    api_key = s.google_maps_api_key
    if not api_key:
        raise RuntimeError("GOOGLE_MAPS_API_KEY is missing in .env")

    url = f"{PLACES_DETAILS_BASE_URL}{place_id}"

    data = get_json(
        url,
        method="GET",
        headers=_details_headers(api_key),
        timeout=30,
    )

    return _parse_place_details(data)


def enrich_places(
    places: List[PlaceLite],
    *,
    limit: Optional[int] = None,
    # Safety knobs:
    sleep_seconds: float = 0.0,
    log_failures: bool = True,
) -> List[PlaceDetails]:
    """
    Convenience helper:
    Take discovery results and enrich each place_id via Place Details.

    limit:
      Optional safety cap to avoid accidental huge runs while testing.

    sleep_seconds:
      Optional throttle between details requests (useful if you hit rate limits).

    log_failures:
      If True, prints a minimal message on a failed place_id (v1-friendly visibility).
    """
    enriched: List[PlaceDetails] = []
    n = len(places) if limit is None else min(len(places), limit)

    for i in range(n):
        pid = places[i].place_id
        try:
            enriched.append(get_place_details(pid))
        except Exception as e:
            if log_failures:
                print(f"[enrich_places] Failed place_id={pid}: {e}")
            continue

        if sleep_seconds and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return enriched