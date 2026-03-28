# Gorgias Growth Analytics Engineer — Case Study

---

## Pipeline Overview

```
setup/ (schema + leads.csv) → data/ scraper → BigQuery → data/ enrichment → Streamlit dashboard
```

| Step | What it does | Where |
|------|-------------|-------|
| 1. Init | Tables schema, columns insertion, initial leads load | `setup/` |
| 2. Extraction | Scrapes Trustpilot reviews for target domains | `data/scraping.py` |
| 3. Cleaning & Storage | Clean, typed data functionalities | `data/cleaning.py` |
| 4. AI Enrichment | Runs Gemini analysis on each review | `data/enrichment.py` |
| 5. Dashboard | Streamlit app for sales rep exploration | `dashboard/` |

---

## Step 1 — Data Extraction

**Script:** `data/scraping.py`

Reviews are scraped from Trustpilot's public-facing website using `curl_cffi` (which handles TLS fingerprinting to avoid bot detection). The scraper iterates over the leads loaded in BQ and paginates through each domain's review pages.

**Fields extracted per review:**

- `domain` — merchant domain
- `text` — full review body
- `title` — review title
- `star_rating` — integer 1–5
- `date_published` — ISO date
- `reviewer_name`
- `company_replied` — boolean

Two fields have been added in order to handle edge-cases:

- `has_text` - boolean
- `language` - review language

**Coverage:** ~150 reviews across 4 domains.

---

## Step 2 — Data Cleaning & Storage

**Script:** `data/cleaning.py`, `data/scraping.py`

Raw scraped data is cleaned and loaded into BQ under the `gorgias-case-study` project with the following transformations:

- Dates parsed to `DATE` type
- Star ratings cast to `INTEGER`
- Non-English reviews flagged via `langdetect` and excluded
- Missing review text handled with a `NULL` guard
- Duplicate reviews de-duplicated on `(domain, reviewer_name, date_published)`

**Validation & fixes** (`sql/`):

The SQL file runs in two phases: a sanity report → automated fixes → a second sanity report to confirm the fixes worked.

#### Phase 1 — Sanity report (pre-fix)

Checks the table for every class of data quality issue before data manipulation:

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNTIF(domain IS NULL) AS null_domains,
    COUNTIF(star_rating IS NULL) AS null_ratings,
    COUNTIF(star_rating NOT BETWEEN 1 AND 5) AS out_of_range,
    MIN(star_rating) AS min_rating,
    MAX(star_rating) AS max_rating,
    COUNTIF(date_published IS NULL) as null_dates,
    COUNTIF(date_published > CURRENT_TIMESTAMP()) as future_dates,
    COUNTIF(date_published < TIMESTAMP('2010-01-01')) as past_dates,
    MIN(date_published) as min_date,
    MAX(date_published) as max_date,
    COUNTIF(text IS NULL AND title IS NULL) AS empty_rev,
    COUNTIF(has_text=TRUE AND text IS NULL) AS mismatch,
    COUNTIF(has_text=FALSE AND text IS NOT NULL) as inv_mismatch,
    COUNT(*) - COUNT(DISTINCT CONCAT(
        COALESCE(domain, ''),
        CAST(date_published AS STRING),
        COALESCE(reviewer_name, '')
    )) AS double_candidates
FROM `gorgias-case-study-491217.lead_enrichment.reviews`;
```

#### Phase 2 — Automated fixes

Four targeted `UPDATE`/`DELETE` statements correct the issues found above:

```sql
-- Clamp out-of-range star ratings to [1, 5]
UPDATE `gorgias-case-study-491217.lead_enrichment.reviews`
SET star_rating = CASE
    WHEN star_rating < 1 THEN 1
    WHEN star_rating > 5 THEN 5
END
WHERE star_rating NOT BETWEEN 1 AND 5 AND star_rating NOT NULL;

-- Null out implausible dates (before 2010)
UPDATE `gorgias-case-study-491217.lead_enrichment.reviews`
SET date_published = NULL
WHERE date_published < TIMESTAMP('2010-01-01');

-- Reconcile has_text flag with actual text content
UPDATE `gorgias-case-study-491217.lead_enrichment.reviews`
SET has_text = (text IS NOT NULL AND TRIM(text) != '')
WHERE has_text != (text IS NOT NULL AND TRIM(text) != '');

-- Delete fully empty rows (all key fields NULL)
DELETE FROM `gorgias-case-study-491217.lead_enrichment.reviews`
WHERE domain IS NULL AND date_published IS NULL
    AND star_rating IS NULL AND text IS NULL AND title IS NULL;
```

#### Phase 3 — Post-fix sanity report

Re-runs the same checks to confirm all issues are resolved.

#### Validation queries

```sql
-- Review count, avg rating, and reply rate per domain
SELECT
    domain,
    COUNT(*) AS total_reviews,
    ROUND(AVG(star_rating), 2) AS avg_rating,
    COUNTIF(has_text AND text IS NOT NULL) AS reviews_with_text,
    COUNTIF(company_replied) AS company_replies
FROM `gorgias-case-study-491217.lead_enrichment.reviews`
WHERE domain IS NOT NULL
GROUP BY domain
HAVING COUNT(star_rating) > 0
ORDER BY total_reviews DESC;

-- Star rating distribution per domain (% breakdown)
SELECT
    domain,
    star_rating,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER (PARTITION BY domain), 1) AS pct_of_domain
FROM `gorgias-case-study-491217.lead_enrichment.reviews`
WHERE domain IS NOT NULL AND star_rating BETWEEN 1 AND 5
GROUP BY domain, star_rating
ORDER BY domain, star_rating DESC;

-- Language distribution (English vs. other)
SELECT
    COALESCE(language, 'unknown') AS language,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(), 1) AS pct_of_total
FROM `gorgias-case-study-491217.lead_enrichment.reviews`
WHERE has_text = TRUE AND text IS NOT NULL
GROUP BY language
ORDER BY count DESC;

-- Monthly review volume and avg rating per domain
SELECT
    domain,
    FORMAT_TIMESTAMP('%Y-%m', date_published) AS month,
    COUNT(*) AS total_reviews,
    ROUND(AVG(star_rating), 2) AS avg_rating
FROM `gorgias-case-study-491217.lead_enrichment.reviews`
WHERE domain IS NOT NULL
    AND date_published IS NOT NULL
    AND date_published BETWEEN TIMESTAMP('2015-01-01') AND CURRENT_TIMESTAMP()
GROUP BY domain, month
HAVING COUNT(*) > 0
ORDER BY domain, month DESC;
```

#### Run the queries in BQ

1. Open the [BQ Console](https://console.cloud.google.com/BQ) and select the `gorgias-case-study-491217` project
2. Open a new query tab and paste the contents of `sql/validation.sql`
3. Run the **Phase 1 sanity report** first to inspect raw data quality
4. Run the **fix statements** one by one (or all at once — they are idempotent)
5. Run the **Phase 3 sanity report** to confirm all counts are zero
6. Run the individual **validation queries** to explore the cleaned dataset

---

## Step 3 — AI Enrichment

**Script:** `data/enrichment.py`  
**Model:** Gemini via `google-genai`

Each review is sent to Gemini with a structured prompt that returns a JSON object with four fields:

```json
{
  "sentiment": "positive | negative | neutral",
  "category": [
    "customer_support", "pricing", "ease_of_use", "integrations", "reliability", "onboarding", 
    "features", "billing", "performance", "other"
    ],
  "pain_point": "One-sentence summary of the core issue (negative reviews only, else null)",
  "insight": "One sentence on what the merchant could do to improve"
}
```

**Prompt design choices:**
- Output is constrained to JSON only to make parsing reliable
- Reviews are batched to reduce API calls and keep costs low
- `langdetect` pre-filters non-English reviews before they reach the LLM, avoiding wasted tokens
- A wait timer is set to 45 seconds to avoid RPM limit

The enriched output is written back to BQ merged into the reviews table.

---

## Step 4 — Dashboard

**Live app:** [gorgias-case-study-qyaznplzq9b5t5ywbmqsnc.streamlit.app](https://gorgias-case-study-qyaznplzq9b5t5ywbmqsnc.streamlit.app/)

**Code:** `dashboard/`  
**Stack:** Streamlit + Plotly + BQ

The dashboard is designed for a Gorgias sales rep preparing for a call with a merchant.

### What it shows

**Overview tab**
- Average star rating per domain
- Total review count
- Sentiment distribution (% positive / negative / neutral)

**Category Breakdown**
- Most common review topics per domain (bar chart)
- Helps identify what customers talk about most

**Pain Point Spotlight**
- Surfaces the top pain points for domains with negative reviews

**Drill-down**
- Click into any domain to see individual reviews

---

## Repo Structure

```
gorgias-case-study/
├── .github/
│   └── workflows/        # CI/CD (e.g. automated scrape/enrichment runs)
├── data/                 # Data manipulation and BQ insertions/merges
├── dashboard/            # Streamlit app
├── setup/                # BQ table creation
├── sql/                  # Validation queries
├── requirements.txt
└── README.md
```

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| `curl_cffi` | TLS-aware HTTP client for Trustpilot scraping |
| `google-cloud-BQ` | Data warehouse storage & querying |
| `google-genai` | Gemini LLM for review enrichment |
| `langdetect` | Filter non-English reviews before enrichment |
| `streamlit` | Dashboard app |
| `plotly` | Charts in the dashboard |