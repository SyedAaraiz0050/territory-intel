```markdown
# Territory Intelligence System  
## B2B Sales Infrastructure – Newfoundland & Labrador

---

## Executive Summary

The Territory Intelligence System is a production-grade, persistent B2B sales engine built to create structural advantage in Newfoundland & Labrador.

It converts real-world physical businesses into a ranked, call-ready opportunity database using:

- Google Places API (New)
- SQLite persistence
- Website text extraction
- OpenAI structured classification
- Mobility-first scoring logic
- Clean CSV export for execution

This system is infrastructure — not a one-off script.

---

## Strategic Objective

### Product Priority

1. **Mobility** (Primary Revenue Driver)
2. **Security**
3. **VoIP**
4. **Fleet Management** (Attach Model)

### Ideal Customer Profile (ICP)

- Field-based operators
- Skilled trades (plumbing, electrical, HVAC, fire protection)
- Industrial service providers
- Logistics and warehousing
- Marine and fisheries
- Dispatch-driven or after-hours businesses

Fleet ownership is optional — not required.

---

## High-Level Architecture

```

Google Places API (New)
├── Text Search (Discovery)
└── Place Details (Enrichment)
↓
SQLite Territory Database
↓
Website Text Extraction
↓
OpenAI Structured Classification
↓
Mobility-First Scoring Model
↓
Ranked CSV Export

```

---

## Repository Structure

```

territory-intel/
├── src/
│   ├── google_places.py
│   ├── store.py
│   ├── classifier.py
│   ├── scoring.py
│   ├── config.py
│   └── utils/
├── scripts/
│   ├── run_all.py
│   ├── test_run_all.py
│   ├── init_test_db.py
│   └── classify_from_db.py
├── data/
│   └── exports/
├── .env
├── .gitignore
└── README.md

```

---

## Core Components

### 1. Google Discovery Layer  
**File:** `src/google_places.py`

- Uses Places API (New)
- Field mask enforcement
- Pagination handling
- NL location bias
- Structured dataclasses
- No raw JSON leakage

---

### 2. Persistence Layer  
**File:** `src/store.py`  
**Database:** SQLite

Key properties:

- `place_id` as PRIMARY KEY
- Idempotent upserts
- Classification caching
- Website hash change detection
- `first_seen` / `last_seen` tracking

Database acts as long-term territory memory.

---

### 3. Website Extraction  
**File:** `src/classifier.py`

- Homepage-only extraction (v1 constraint)
- Defensive timeout handling
- Text-only processing

---

### 4. OpenAI Classification

Structured output fields:

- `industry_bucket`
- `mobility_fit`
- `security_fit`
- `voip_fit`
- `fleet_attach`
- `signal_after_hours`
- `signal_dispatch`
- `signal_field_work`
- `ai_reason`

Strict structured JSON enforcement.

---

### 5. Scoring Engine  
**File:** `src/scoring.py`

Mobility-first weighted scoring model.

Inputs:

- Product fit levels
- Rating
- Review count
- Website presence
- Opening hours presence

Output:

- `total_score`

Sorted descending for dialing priority.

---

## Execution Modes

### Full Territory Run

```

python -m scripts.run_all

```

- Province-wide discovery
- Enrichment of new businesses
- Classification cap (e.g., 200 per run)
- Ranked export
- Google + OpenAI usage

---

### DB-Only Classification (No Google)

```

python -m scripts.classify_from_db --limit 200

```

- Classifies only previously unclassified businesses
- No Google API calls
- Incremental AI enrichment
- Cost-efficient scaling

---

### Sanity Test Run

```

python -m scripts.init_test_db
python -m scripts.test_run_all

```

- St. John’s-only scope
- Limited classification (e.g., 50)
- End-to-end pipeline validation

---

## Output Format

Exports located in:

```

data/exports/

```

Primary export structure:

- name
- phone
- website
- address
- primary_type
- industry_bucket
- mobility_fit
- security_fit
- voip_fit
- fleet_attach
- rating
- review_count
- total_score
- ai_reason

Sorted by:

```

total_score DESC

```

Designed for execution, not analysis.

---

## Cost Control Mechanisms

- Place ID deduplication
- Enrichment only when missing
- AI classification caching
- Website hash comparison
- Classification cap per run
- DB-only classification mode

---

## Operational Workflow

Weekly:

```

python -m scripts.run_all

```

Daily:

```

python -m scripts.classify_from_db --limit 200

```

Territory intelligence compounds over time.

---

## Technology Stack

- Python 3.11+
- SQLite
- Google Places API (New)
- OpenAI API
- requests
- pandas
- pydantic
- tenacity
- trafilatura

---

## Design Principles

- Persistent data model
- Cost-aware execution
- Structured AI outputs only
- Mobility-first bias
- Execution-oriented exports
- Clear separation of discovery and classification

---

## Roadmap

### v1
- Province-wide discovery
- Homepage extraction
- AI classification
- Weighted scoring
- Ranked export

### v2
- Multi-page extraction
- Freshness gating
- Search-run logging
- Daily call-sheet generator

### v3
- Change detection
- Territory heat mapping
- Competitive carrier inference
- Revenue projection modeling

---

## Strategic Advantage

Most B2B sales workflows rely on:

- Manual Google searches
- Fragmented LinkedIn coverage
- Unranked lead lists

This system provides:

- Structured territory coverage
- Automated prioritization
- Persistent intelligence
- Repeatable execution
- Compounding advantage

---

## Author Context

Built for Newfoundland & Labrador territory execution by a technical B2B telecom operator applying engineering discipline to sales systems.
```
