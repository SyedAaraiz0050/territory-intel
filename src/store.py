# src/store.py
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

from src.config import get_settings


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class Store:
    """
    Persistence layer ONLY.

    Responsibilities:
    - Create/open SQLite DB
    - Create schema
    - Upsert places by place_id
    - Cache decisions:
        - needs_details (Google cost control)
        - should_classify (OpenAI cost control)
    - Store AI + score fields

    Non-goals:
    - No Google calls
    - No OpenAI calls
    - No scoring math
    """

    def __init__(self, db_path: Optional[str] = None) -> None:
        s = get_settings()
        resolved = Path(db_path or s.db_path).resolve()
        resolved.parent.mkdir(parents=True, exist_ok=True)

        self.db_path = resolved
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.execute("PRAGMA journal_mode = WAL;")
        self.conn.execute("PRAGMA synchronous = NORMAL;")

    def close(self) -> None:
        self.conn.close()

    # -----------------------------
    # Schema
    # -----------------------------
    def init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS places (
                place_id TEXT PRIMARY KEY,
                name TEXT,
                address TEXT,
                phone TEXT,
                website TEXT,
                rating REAL,
                review_count INTEGER,
                lat REAL,
                lng REAL,
                primary_type TEXT,
                types_json TEXT,
                business_status TEXT,
                maps_url TEXT,
                opening_hours_json TEXT,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,

                -- AI
                industry_bucket TEXT,
                mobility_fit INTEGER,
                security_fit INTEGER,
                voip_fit INTEGER,
                fleet_attach INTEGER,

                signal_after_hours INTEGER,
                signal_dispatch INTEGER,
                signal_field_work INTEGER,

                ai_reason TEXT,
                ai_last_updated TEXT,

                -- scoring
                total_score REAL
            );

            CREATE INDEX IF NOT EXISTS idx_places_last_seen ON places(last_seen);
            CREATE INDEX IF NOT EXISTS idx_places_primary_type ON places(primary_type);
            CREATE INDEX IF NOT EXISTS idx_places_rating ON places(rating);
            CREATE INDEX IF NOT EXISTS idx_places_website ON places(website);
            """
        )
        self.conn.commit()

    # -----------------------------
    # Dedupe helpers
    # -----------------------------
    def existing_place_ids(self, place_ids: Sequence[str]) -> Set[str]:
        if not place_ids:
            return set()

        existing: Set[str] = set()
        CHUNK = 800
        for i in range(0, len(place_ids), CHUNK):
            chunk = place_ids[i : i + CHUNK]
            q = f"SELECT place_id FROM places WHERE place_id IN ({','.join(['?'] * len(chunk))})"
            rows = self.conn.execute(q, chunk).fetchall()
            existing.update(r["place_id"] for r in rows)
        return existing

    def touch_last_seen(self, place_ids: Iterable[str]) -> None:
        """
        FIXED: accepts list/set/tuple/any iterable safely.
        Converts to list so slicing works.
        """
        ids = list(place_ids)
        if not ids:
            return

        now = _utc_now_iso()
        CHUNK = 800
        for i in range(0, len(ids), CHUNK):
            chunk = ids[i : i + CHUNK]
            q = f"UPDATE places SET last_seen=? WHERE place_id IN ({','.join(['?'] * len(chunk))})"
            self.conn.execute(q, [now, *chunk])
        self.conn.commit()

    # -----------------------------
    # Core upsert
    # -----------------------------
    def upsert_place(
        self,
        place_id: str,
        *,
        name: Optional[str] = None,
        address: Optional[str] = None,
        phone: Optional[str] = None,
        website: Optional[str] = None,
        rating: Optional[float] = None,
        review_count: Optional[int] = None,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        primary_type: Optional[str] = None,
        types: Optional[List[str]] = None,
        business_status: Optional[str] = None,
        maps_url: Optional[str] = None,
        opening_hours_json: Optional[Dict[str, Any]] = None,
    ) -> None:
        now = _utc_now_iso()
        types_json = json.dumps(types, ensure_ascii=False) if types is not None else None
        hours_json = json.dumps(opening_hours_json, ensure_ascii=False) if opening_hours_json is not None else None

        self.conn.execute(
            """
            INSERT INTO places (
                place_id, name, address, phone, website, rating, review_count,
                lat, lng, primary_type, types_json, business_status,
                maps_url, opening_hours_json,
                first_seen, last_seen
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?,
                ?, ?
            )
            ON CONFLICT(place_id) DO UPDATE SET
                last_seen = excluded.last_seen,

                name = COALESCE(excluded.name, places.name),
                address = COALESCE(excluded.address, places.address),
                phone = COALESCE(excluded.phone, places.phone),
                website = COALESCE(excluded.website, places.website),
                rating = COALESCE(excluded.rating, places.rating),
                review_count = COALESCE(excluded.review_count, places.review_count),
                lat = COALESCE(excluded.lat, places.lat),
                lng = COALESCE(excluded.lng, places.lng),
                primary_type = COALESCE(excluded.primary_type, places.primary_type),
                types_json = COALESCE(excluded.types_json, places.types_json),
                business_status = COALESCE(excluded.business_status, places.business_status),
                maps_url = COALESCE(excluded.maps_url, places.maps_url),
                opening_hours_json = COALESCE(excluded.opening_hours_json, places.opening_hours_json)
            ;
            """,
            (
                place_id,
                name,
                address,
                phone,
                website,
                rating,
                review_count,
                lat,
                lng,
                primary_type,
                types_json,
                business_status,
                maps_url,
                hours_json,
                now,
                now,
            ),
        )
        self.conn.commit()

    # -----------------------------
    # Google enrichment cache decision
    # -----------------------------
    def needs_details(self, place_id: str) -> bool:
        """
        v1: do NOT chase opening hours forever (often missing).
        Only force details if missing call-ready essentials.
        """
        row = self.conn.execute(
            """
            SELECT phone, maps_url, website
            FROM places
            WHERE place_id=?
            """,
            (place_id,),
        ).fetchone()

        if row is None:
            return True

        if row["phone"] is None:
            return True
        if row["maps_url"] is None:
            return True

        # website is useful but not required; keep it optional for v1
        return False

    # -----------------------------
    # AI persistence + cache decision
    # -----------------------------
    def upsert_ai(
        self,
        place_id: str,
        *,
        industry_bucket: str,
        mobility_fit: int,
        security_fit: int,
        voip_fit: int,
        fleet_attach: int,
        signal_after_hours: int,
        signal_dispatch: int,
        signal_field_work: int,
        ai_reason: str,
    ) -> None:
        now = _utc_now_iso()
        self.conn.execute(
            """
            UPDATE places
            SET
                industry_bucket=?,
                mobility_fit=?,
                security_fit=?,
                voip_fit=?,
                fleet_attach=?,
                signal_after_hours=?,
                signal_dispatch=?,
                signal_field_work=?,
                ai_reason=?,
                ai_last_updated=?
            WHERE place_id=?
            """,
            (
                industry_bucket,
                int(mobility_fit),
                int(security_fit),
                int(voip_fit),
                int(fleet_attach),
                int(signal_after_hours),
                int(signal_dispatch),
                int(signal_field_work),
                ai_reason[:400],
                now,
                place_id,
            ),
        )
        self.conn.commit()

    def get_ai_state(self, place_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT website, ai_last_updated, mobility_fit, security_fit, voip_fit, fleet_attach
            FROM places
            WHERE place_id=?
            """,
            (place_id,),
        ).fetchone()

    def should_classify(self, place_id: str, current_website: Optional[str]) -> bool:
        row = self.get_ai_state(place_id)
        if row is None:
            return True
        if row["ai_last_updated"] is None:
            return True
        for k in ("mobility_fit", "security_fit", "voip_fit", "fleet_attach"):
            if row[k] is None:
                return True

        stored_website = row["website"]
        if current_website and stored_website and current_website != stored_website:
            return True
        if current_website and not stored_website:
            return True
        return False

    # -----------------------------
    # Reads / exports
    # -----------------------------
    def fetch_rows_for_classification(self, limit: int = 50_000) -> List[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT place_id, name, address, website, primary_type
            FROM places
            ORDER BY last_seen DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()

    def upsert_score(self, place_id: str, total_score: float) -> None:
        self.conn.execute(
            "UPDATE places SET total_score=? WHERE place_id=?",
            (float(total_score), place_id),
        )
        self.conn.commit()

    def fetch_export_rows(self) -> List[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT
              place_id, name, address, phone, website,
              rating, review_count, primary_type, business_status,
              maps_url, opening_hours_json,

              industry_bucket,
              mobility_fit, security_fit, voip_fit, fleet_attach,
              signal_after_hours, signal_dispatch, signal_field_work,
              ai_reason, ai_last_updated,

              total_score,
              first_seen, last_seen
            FROM places
            WHERE business_status IS NULL OR business_status != 'CLOSED_PERMANENTLY'
            """
        ).fetchall()