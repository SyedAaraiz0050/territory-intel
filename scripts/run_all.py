from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import List, Set

from src.google_places import text_search, get_place_details
from src.store import Store
from src.classifier import classify_business, fetch_homepage_text
from src.scoring import compute_score


DB_NAME = "territory.db"
EXPORT_PATH = Path("data/exports/nl_full_ranked.csv")

# Full NL Coverage
CITIES: List[str] = [
    "St. John's NL",
    "Mount Pearl NL",
    "Paradise NL",
    "Conception Bay South NL",
    "Gander NL",
    "Grand Falls-Windsor NL",
    "Corner Brook NL",
    "Stephenville NL",
    "Deer Lake NL",
    "Labrador City NL",
    "Happy Valley-Goose Bay NL",
    "Channel-Port aux Basques NL",
    "Clarenville NL",
    "Bay Roberts NL",
]

KEYWORDS: List[str] = [
    "plumber",
    "electrician",
    "hvac",
    "industrial services",
    "property maintenance",
    "logistics",
    "warehouse",
    "construction company",
    "towing service",
    "locksmith",
    "security system supplier",
    "marine services",
    "fisheries",
]

MAX_CLASSIFICATIONS = 200


def export_csv(rows, path: Path) -> None:
    if not rows:
        print("[EXPORT] No rows to export.")
        return

    path.parent.mkdir(parents=True, exist_ok=True)

    dict_rows = [dict(r) for r in rows]
    dict_rows.sort(key=lambda x: (x.get("total_score") or 0), reverse=True)

    # Select readable columns in proper order
    columns = [
        "name",
        "phone",
        "website",
        "address",
        "primary_type",
        "industry_bucket",
        "mobility_fit",
        "security_fit",
        "voip_fit",
        "fleet_attach",
        "rating",
        "review_count",
        "total_score",
        "ai_reason",
    ]

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for r in dict_rows:
            writer.writerow(r)

    print(f"[EXPORT] Clean ranked list written to: {path.resolve()}")
    print(f"[EXPORT] Total rows: {len(dict_rows)}")


def main():
    os.environ["DB_PATH"] = DB_NAME

    store = Store(db_path=DB_NAME)
    store.init_schema()

    print(f"[DB] Using: {Path(DB_NAME).resolve()}")

    discovered_ids: Set[str] = set()

    # -----------------------------
    # DISCOVERY (Province Wide)
    # -----------------------------
    for city in CITIES:
        for kw in KEYWORDS:
            query = f"{kw} in {city}"
            print(f"\n[DISCOVERY] {query}")

            try:
                places = text_search(query, max_pages=3)
            except Exception as e:
                print(f"[DISCOVERY ERROR] {query} -> {e}")
                continue

            print(f"  -> found={len(places)}")

            for p in places:
                discovered_ids.add(p.place_id)
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

    print(f"\n[DISCOVERY] Unique businesses discovered: {len(discovered_ids)}")

    # -----------------------------
    # DETAILS ENRICHMENT
    # -----------------------------
    need_details = [pid for pid in discovered_ids if store.needs_details(pid)]
    print(f"[DETAILS] Needs enrichment: {len(need_details)}")

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

            if idx % 25 == 0 or idx == len(need_details):
                print(f"[DETAILS] {idx}/{len(need_details)}")

        except Exception as e:
            print(f"[DETAILS ERROR] {pid} -> {e}")

    # -----------------------------
    # CLASSIFICATION (Max 200 NEW)
    # -----------------------------
    rows = store.fetch_rows_for_classification(limit=10000)

    classified = 0

    for r in rows:
        if classified >= MAX_CLASSIFICATIONS:
            break

        pid = r["place_id"]

        if not store.should_classify(pid, r["website"]):
            continue

        homepage_text = None
        if r["website"]:
            try:
                homepage_text = fetch_homepage_text(r["website"])
            except Exception:
                homepage_text = None

        try:
            result = classify_business(
                name=r["name"] or "",
                address=r["address"] or "",
                primary_type=r["primary_type"],
                website=r["website"],
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
                has_website=bool(row2["website"]) if row2 else False,
                has_opening_hours=bool(row2["opening_hours_json"]) if row2 else False,
            )

            store.upsert_score(pid, score)

            classified += 1
            print(f"[AI] {classified}/{MAX_CLASSIFICATIONS} | {r['name']} | Score={score:.1f}")

        except Exception as e:
            print(f"[AI ERROR] {r['name']} -> {e}")

    # -----------------------------
    # EXPORT CLEAN CSV
    # -----------------------------
    export_rows = store.fetch_export_rows()
    export_csv(export_rows, EXPORT_PATH)

    print("\n=== RUN COMPLETE ===")
    print(f"New classifications this run: {classified}")

    store.close()


if __name__ == "__main__":
    main()