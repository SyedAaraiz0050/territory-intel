# scripts/test_run_all.py
from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Dict, List, Set

from src.google_places import get_place_details, text_search
from src.store import Store
from src.classifier import classify_business, fetch_homepage_text
from src.scoring import compute_score

TEST_DB = "territory_test.db"
EXPORT_PATH = Path("data/exports/stjohns_test.csv")

# Keep this small for sanity test
CITY_QUERY = "St. John's NL"
KEYWORDS: List[str] = [
    "plumber",
    "electrician",
    "hvac",
    "property maintenance",
    "industrial services",
    "logistics",
    "warehouse",
    "security",
    "locksmith",
]

MAX_CLASSIFICATIONS = 50


def export_csv(rows, path: Path) -> None:
    if not rows:
        print("[EXPORT] No rows to export.")
        return

    path.parent.mkdir(parents=True, exist_ok=True)

    # sqlite3.Row supports dict(row)
    dict_rows = [dict(r) for r in rows]

    # Sort by score descending (None treated as 0)
    dict_rows.sort(key=lambda x: (x.get("total_score") or 0), reverse=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(dict_rows[0].keys()))
        w.writeheader()
        for r in dict_rows:
            w.writerow(r)

    print(f"[EXPORT] Wrote: {path.resolve()} | rows={len(dict_rows)}")


def main() -> None:
    # Force test DB without touching your main DB
    os.environ["DB_PATH"] = TEST_DB

    # (Optional) start clean each run
    # If you want to keep results between runs, comment these 2 lines.
    dbp = Path(TEST_DB)
    if dbp.exists():
        dbp.unlink()

    store = Store(db_path=TEST_DB)
    store.init_schema()
    print(f"[DB] Ready: {Path(TEST_DB).resolve()}")

    # -------------------------
    # 1) DISCOVERY (St. John's only)
    # -------------------------
    all_place_ids: Set[str] = set()

    for i, kw in enumerate(KEYWORDS, start=1):
        q = f"{kw} in {CITY_QUERY}"
        print(f"\n[DISCOVERY {i}/{len(KEYWORDS)}] {q}")

        try:
            places = text_search(q, max_pages=2)
        except Exception as e:
            print(f"  [DISCOVERY ERROR] {q} -> {e}")
            continue

        print(f"  -> found={len(places)}")

        for p in places:
            all_place_ids.add(p.place_id)
            store.upsert_place(
                p.place_id,
                name=p.name,
                address=p.address,
                lat=p.lat,
                lng=p.lng,
                primary_type=p.primary_type,
                types=p.types,
                business_status=p.business_status,
            )

    print(f"\n[DISCOVERY] Unique places collected: {len(all_place_ids)}")

    # -------------------------
    # 2) ENRICH DETAILS (phone/maps_url are "call-ready essentials")
    # -------------------------
    need_details = [pid for pid in all_place_ids if store.needs_details(pid)]
    print(f"[DETAILS] Needs details: {len(need_details)}")

    for idx, pid in enumerate(need_details, start=1):
        try:
            d = get_place_details(pid)
            store.upsert_place(
                d.place_id,
                name=d.name,
                address=d.address,
                phone=d.phone,
                website=d.website,
                rating=d.rating,
                review_count=d.review_count,
                lat=d.lat,
                lng=d.lng,
                primary_type=d.primary_type,
                types=d.types,
                business_status=d.business_status,
                maps_url=d.maps_url,
                opening_hours_json=d.opening_hours_json,
            )
            if idx % 10 == 0 or idx == len(need_details):
                print(f"  [DETAILS] progress {idx}/{len(need_details)}")
        except Exception as e:
            print(f"  [DETAILS ERROR] {pid} -> {e}")

    # -------------------------
    # 3) CLASSIFY (max 50)
    # -------------------------
    rows = store.fetch_rows_for_classification(limit=5000)
    print(f"\n[AI] Candidates in DB: {len(rows)} | Target classifications: {MAX_CLASSIFICATIONS}")

    classified_ok = 0
    classified_fail = 0
    classified_skip = 0

    for idx, r in enumerate(rows, start=1):
        if classified_ok >= MAX_CLASSIFICATIONS:
            break

        pid = r["place_id"]
        name = r["name"] or ""
        website = r["website"]

        if not store.should_classify(pid, website):
            classified_skip += 1
            continue

        # Homepage fetch is optional (website can be None)
        homepage_text = None
        if website:
            try:
                print(f"[AI {classified_ok + 1}/{MAX_CLASSIFICATIONS}] Fetch homepage: {name}")
                homepage_text = fetch_homepage_text(website)
            except Exception as e:
                homepage_text = None
                print(f"  [WEB WARN] homepage fetch failed: {e}")

        try:
            print(f"[AI {classified_ok + 1}/{MAX_CLASSIFICATIONS}] Classify: {name}")
            result = classify_business(
                name=name,
                address=r["address"] or "",
                primary_type=r["primary_type"],
                website=website,
                homepage_text=homepage_text,
            )

            store.upsert_ai(
                pid,
                industry_bucket=result.industry_bucket,
                mobility_fit=result.mobility_fit,
                security_fit=result.security_fit,
                voip_fit=result.voip_fit,
                fleet_attach=result.fleet_attach,
                signal_after_hours=result.signal_after_hours,
                signal_dispatch=result.signal_dispatch,
                signal_field_work=result.signal_field_work,
                ai_reason=result.ai_reason,
            )

            # Pull deterministic boosts from DB (rating/reviews/website/hours)
            row2 = store.conn.execute(
                "SELECT rating, review_count, website, opening_hours_json FROM places WHERE place_id=?",
                (pid,),
            ).fetchone()

            score = compute_score(
                mobility_fit=result.mobility_fit,
                security_fit=result.security_fit,
                voip_fit=result.voip_fit,
                fleet_attach=result.fleet_attach,
                rating=row2["rating"] if row2 else None,
                review_count=row2["review_count"] if row2 else None,
                has_website=bool(row2["website"]) if row2 else bool(website),
                has_opening_hours=bool(row2["opening_hours_json"]) if row2 else False,
            )

            store.upsert_score(pid, score)

            classified_ok += 1
            print(
                f"  [AI OK] score={score:.1f} | "
                f"M={result.mobility_fit} S={result.security_fit} V={result.voip_fit} F={result.fleet_attach} | "
                f"{name}"
            )

        except Exception as e:
            classified_fail += 1
            print(f"  [AI ERROR] {name} -> {e}")

    # -------------------------
    # 4) EXPORT
    # -------------------------
    export_rows = store.fetch_export_rows()
    export_csv(export_rows, EXPORT_PATH)

    print("\n==== TEST SUMMARY ====")
    print(f"DB: {Path(TEST_DB).resolve()}")
    print(f"Unique discovered: {len(all_place_ids)}")
    print(f"Details fetched: {len(need_details)}")
    print(f"AI classified OK: {classified_ok}")
    print(f"AI skipped (cached): {classified_skip}")
    print(f"AI failed: {classified_fail}")

    store.close()


if __name__ == "__main__":
    main()