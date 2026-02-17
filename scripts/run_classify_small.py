# scripts/run_classify_small.py
from __future__ import annotations

from src.classifier import classify_business, fetch_homepage_text
from src.google_places import get_place_details, text_search
from src.store import Store


# Small, controlled run (low spend)
QUERY = "electrician in St. John's NL"
MAX_PAGES = 1          # Google discovery cap
DETAILS_LIMIT = 10     # max Google details calls
CLASSIFY_LIMIT = 10    # max OpenAI calls
HOMEPAGE_TEXT_CHARS = 10_000  # homepage text cap


def main() -> None:
    store = Store()
    store.init_schema()

    # 1) Discovery
    places = text_search(QUERY, max_pages=MAX_PAGES)
    print(f"[DISCOVERY] query='{QUERY}' results={len(places)}")

    place_ids = [p.place_id for p in places if p.place_id]

    # IMPORTANT: check existing BEFORE upsert, so "new" is real
    existing_before = store.existing_place_ids(place_ids)
    new_ids = [pid for pid in place_ids if pid not in existing_before]

    # Touch last_seen for existing (no API cost)
    store.touch_last_seen(list(existing_before))

    # 2) Upsert lite (core) for all
    for p in places:
        store.upsert_place(
            p.place_id,
            name=p.name or None,
            address=p.address or None,
            lat=p.lat,
            lng=p.lng,
            primary_type=p.primary_type,
            types=p.types,
            business_status=p.business_status,
        )

    # 3) Enrichment: details only for (new OR missing fields), bounded by DETAILS_LIMIT
    details_targets = []
    for p in places:
        pid = p.place_id
        if not pid:
            continue
        if pid in new_ids or store.needs_details(pid):
            details_targets.append(pid)

    details_targets = details_targets[:DETAILS_LIMIT]

    details_ok = 0
    details_fail = 0
    for i, pid in enumerate(details_targets, start=1):
        try:
            d = get_place_details(pid)
            store.upsert_place(
                d.place_id,
                name=d.name or None,
                address=d.address or None,
                phone=d.phone,
                website=d.website,
                rating=d.rating,
                review_count=d.review_count,
                maps_url=d.maps_url,
                opening_hours_json=d.opening_hours_json,
                lat=d.lat,
                lng=d.lng,
                primary_type=d.primary_type,
                types=d.types,
                business_status=d.business_status,
            )
            details_ok += 1
            print(f"[DETAILS] {i}/{len(details_targets)} ok {d.place_id} {d.name}")
        except Exception as e:
            details_fail += 1
            print(f"[DETAILS] {i}/{len(details_targets)} FAIL {pid}: {e}")

    # 4) Classification (OpenAI): only when should_classify says yes, bounded by CLASSIFY_LIMIT
    rows = store.fetch_rows_for_classification(limit=200)

    classify_ok = 0
    classify_skip = 0
    classify_fail = 0

    for r in rows:
        if classify_ok >= CLASSIFY_LIMIT:
            break

        place_id = r["place_id"]
        name = r["name"] or ""
        address = r["address"] or ""
        website = r["website"]
        primary_type = r["primary_type"]

        if not store.should_classify(place_id, website):
            classify_skip += 1
            continue

        homepage_text = None
        if website:
            try:
                homepage_text = fetch_homepage_text(website, max_chars=HOMEPAGE_TEXT_CHARS)
            except Exception as e:
                print(f"[HOME] fetch failed {place_id} ({website}): {e}")

        try:
            c = classify_business(
                name=name,
                address=address,
                primary_type=primary_type,
                website=website,
                homepage_text=homepage_text,
                max_output_tokens=250,
            )

            store.upsert_ai(
                place_id,
                industry_bucket=c.industry_bucket,
                mobility_fit=c.mobility_fit,
                security_fit=c.security_fit,
                voip_fit=c.voip_fit,
                fleet_attach=c.fleet_attach,
                signal_after_hours=c.signal_after_hours,
                signal_dispatch=c.signal_dispatch,
                signal_field_work=c.signal_field_work,
                ai_reason=c.ai_reason,
            )

            classify_ok += 1
            print(f"[AI] ok {place_id} mobility={c.mobility_fit} bucket={c.industry_bucket}")
        except Exception as e:
            classify_fail += 1
            print(f"[AI] FAIL {place_id}: {e}")

    print("\n========== SMALL RUN SUMMARY ==========")
    print(f"DB: {store.db_path}")
    print(f"Discovery results: {len(places)}")
    print(f"Existing before: {len(existing_before)} | New: {len(new_ids)}")
    print(f"Details targets: {len(details_targets)} | ok={details_ok} fail={details_fail} (limit={DETAILS_LIMIT})")
    print(f"AI classify: ok={classify_ok} skip={classify_skip} fail={classify_fail} (limit={CLASSIFY_LIMIT})")
    print("Re-run this script: Details + AI should mostly SKIP if caching works.")
    print("======================================\n")

    store.close()


if __name__ == "__main__":
    main()