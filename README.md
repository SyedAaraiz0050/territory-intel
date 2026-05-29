# Territory Intel

A Python pipeline that discovers B2B businesses across Newfoundland and Labrador via the Google Places API (New), enriches them with contact data, classifies telecom-mobility fit using OpenAI, scores them with a deterministic weighted model, and exports a ranked CSV lead sheet — all persisted in a local SQLite database that accumulates intelligence across runs.

Built as a portfolio project by a B2B telecom sales professional transitioning into software and data engineering roles.

---

## The problem it solves

B2B territory management in telecom is largely manual: reps identify businesses ad hoc, lose track of who they have contacted, and have no repeatable way to rank prospects by fit. This pipeline automates discovery and ranking across the full NL province, storing results in SQLite so every run builds on the last — previously classified businesses are skipped, and only new or changed records consume API budget.

---

## Architecture

```
Google Places API (New)
  │
  ├─ text_search()         discovery: place_id + basic metadata
  └─ get_place_details()   enrichment: phone, website, hours, rating
          │
          ▼
    SQLite  (territory.db)
    ├── place_id as primary key
    ├── first_seen / last_seen per record
    ├── needs_details()      Google cost gate
    └── should_classify()    OpenAI cost gate
          │
          ├─ fetch_homepage_text()   single-page fetch (requests)
          ▼
    OpenAI API  (gpt-4.1-mini)
    └── Pydantic-validated Classification
        (mobility_fit, security_fit, voip_fit, fleet_attach,
         signal_after_hours, signal_dispatch, signal_field_work,
         industry_bucket, ai_reason)
          │
          ▼
    scoring.py → compute_score()
    └── Weighted 0–100 score → ranked CSV export
```

---

## Quickstart

**Requirements:** Python 3.11+, a Google Maps API key (Places API New), and an OpenAI API key.

```bash
git clone https://github.com/SyedAaraiz0050/territory-intel.git
cd territory-intel

python -m venv .venv
# macOS/Linux:
source .venv/bin/activate
# Windows:
.venv\Scripts\activate

pip install -e .

# Create a .env file with your keys (see Configuration below)

# Verify keys load correctly
python -m scripts.test_key       # prints loaded Google key
python -m scripts.test_openai    # makes a minimal OpenAI call

# Bounded test run — one query, ~10 Google + ~10 OpenAI calls
python -m scripts.run_classify_small

# Full province run — 14 cities × 13 keywords, up to 200 AI classifications
python -m scripts.run_all
```

---

## Configuration

Create a `.env` file in the project root:

```
GOOGLE_MAPS_API_KEY=your_key_here
OPENAI_API_KEY=your_key_here
DB_PATH=territory.db
LOG_LEVEL=INFO
```

| Variable | Required | Default | Description |
|---|---|---|---|
| `GOOGLE_MAPS_API_KEY` | Yes | — | Places API (New) — Text Search + Place Details |
| `OPENAI_API_KEY` | Yes | — | Used with `gpt-4.1-mini` |
| `DB_PATH` | No | `territory.db` | SQLite file path |
| `LOG_LEVEL` | No | `INFO` | `DEBUG`, `INFO`, or `WARNING` |

---

## Repository structure

```
src/                          # core package
  google_places.py            # Places API (New) — discovery + enrichment
  store.py                    # SQLite persistence (upserts, caching gates)
  classifier.py               # OpenAI classification + Pydantic validation
  scoring.py                  # deterministic weighted score
  config.py                   # settings from .env
  utils/
    http.py                   # HTTP helper with tenacity retries (5 attempts)
    log.py                    # logging setup

scripts/                      # executable entry points
  run_all.py                  # full NL province run (14 cities, 13 keywords)
  classify_from_db.py         # classify stored businesses without Google calls
  run_classify_small.py       # bounded single-query run for low-cost testing
  test_run_all.py             # St. John's-only test run
  init_test_db.py             # create a clean test database
  test_openai.py              # verify OpenAI key works
  test_key.py                 # print loaded Google Maps key

data/
  exports/
    sample_ranked.csv         # synthetic sample output — see below
```

---

## Workflows

### Full territory build

```bash
python -m scripts.run_all
```

Runs province-wide discovery across 14 NL cities and 13 B2B keywords, enriches any records missing a phone number or Maps URL, classifies up to 200 new or changed businesses, and writes `data/exports/nl_full_ranked.csv`.

### DB-only classification

```bash
python -m scripts.classify_from_db
```

Classifies businesses already stored in the database — no Google API calls. Useful for working down a backlog of unclassified records without incurring additional discovery cost.

### Small validation run

```bash
python -m scripts.run_classify_small
```

Single query (`electrician in St. John's NL`), capped at 10 Google detail calls and 10 OpenAI calls. Use this to verify the full pipeline end-to-end before a province-wide run.

---

## How scoring works

`src/scoring.py` — `compute_score()` combines AI fit scores with deterministic signals into a single 0–100 value.

**Weighted fit scores** (from OpenAI classification):

| Dimension | Weight |
|---|---|
| `mobility_fit` | 55 % |
| `security_fit` | 20 % |
| `voip_fit` | 15 % |
| `fleet_attach` | 10 % |

**Deterministic boosts** (each +5, total capped at 100):

| Condition | Bonus |
|---|---|
| Google rating ≥ 4.2 | +5 |
| Review count ≥ 10 | +5 |
| Website present | +5 |
| Opening hours present | +5 |

The mobility weighting reflects the primary revenue driver in the NL territory this project was built for: device plans, hotspots, and field connectivity.

---

## Sample output

[`data/exports/sample_ranked.csv`](data/exports/sample_ranked.csv) contains 15 rows of **synthetic, fictional** businesses — invented names, placeholder phone numbers, `.example` domains, no real place IDs or scraped data. It shows every column the pipeline produces, sorted by `total_score` descending exactly as a real export would be.

Real exports are gitignored (`data/exports/*`) because they contain Google Places API content subject to caching restrictions.

---

## Key implementation details

**Idempotent upserts.** `store.upsert_place()` uses `INSERT … ON CONFLICT(place_id) DO UPDATE SET` with `COALESCE` — existing non-null values are never overwritten by nulls on subsequent runs.

**Cost gates.** `needs_details()` skips the Google Place Details call if a record already has a phone number and Maps URL. `should_classify()` skips the OpenAI call if all four fit scores are present and the website URL hasn't changed.

**Pydantic validation with repair.** The `Classification` model validates the model's JSON output. If strict validation fails, a `_normalize()` function coerces strings, floats, booleans, and percentage strings into valid integers before re-validating.

**Retries.** All Google Places API calls go through `src/utils/http.py`, which wraps `requests` with `tenacity` — up to 5 attempts with exponential backoff. OpenAI calls in `classifier.py` do not retry; a failed classification is logged and skipped.

---

## Data handling & compliance

**Google Places API.** Google's Terms of Service restrict caching and storage of Places API content, including `place_id`, and prohibit displaying that content outside of a Google Map. This database is a local operational cache for a single user's prospecting workflow — do not redistribute or publish its contents.

**Homepage extraction.** Only the single URL returned by Google Places for a business is fetched. No crawling. Text is truncated to 10,000 characters before being sent to the model.

**AI fit scores.** `mobility_fit`, `security_fit`, `voip_fit`, and `fleet_attach` are outputs of `gpt-4.1-mini` with no formal accuracy evaluation. They represent the model's inference from business name, address, type, and homepage text. Treat them as a sorting signal, not a validated prediction.

---

## Roadmap

**v1 (complete).** Province-wide discovery → enrichment → Pydantic-validated OpenAI classification → weighted scoring → ranked CSV export. Persistent SQLite with idempotent upserts and API cost gates.

**v2.** Re-enrichment scheduling for stale records. Configurable keyword and city lists. Weekly/daily run scheduling.

**v3.** Formal classification evaluation harness with a human-labeled sample. CRM or Google Sheets export integration. Competitive carrier signal detection.

---

## What this demonstrates

End-to-end pipeline engineering: external API integration with field-masked requests and pagination, SQL persistence with idempotent upsert logic and cost-gating decisions, LLM structured output with Pydantic validation and a normalization repair path, and a deterministic scoring layer — built for a real prospecting workflow the author ran in the field.

---

## Author

MSc Computer Engineering &nbsp;·&nbsp; B2B Telecom Sales, Newfoundland & Labrador
