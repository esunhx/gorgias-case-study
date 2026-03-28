import json
import os
import re
import time

from google import genai
from google.cloud import bigquery

PROJECT_ID = "gorgias-case-study-491217"
DATASET_ID = "lead_enrichment"
TABLE_ID = "reviews"

bq = bigquery.Client(project=PROJECT_ID)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

BATCH_SIZE = 20

SYSTEM_PROMPT = """
You are a review analysis assistant. You will receive a batch of customer reviews.
For each review, return a JSON array where each object contains exactly these fields:
- sentiment: one of "positive", "negative" or "neutral"
- category: the main topic of the review, one of: 
    "customer_support", "pricing", "ease_of_use", "integrations", "reliability", "onboarding", 
    "features", "billing", "performance", "other"
- pain_point: if sentiment is negative, one sentence to describe the issue, otherwise null.
- insight: one sentence on what the merchant can improve, otherwise null.

RULES:
- Return only a valid JSON array, no preamble, no markdown, no explanation.
- The array must have exaclty the same number of objects as reviews provided.
- Keep pain_point and insight to one sentence.
""".strip()

def build_user_prompt(reviews: list[dict]) -> str:
    review = "\n\n".join(
        f"Review {i+1}:\n"
        f"Domain: {r['domain']}\n"
        f"Star rating: {r['star_rating']}/5\n"
        f"Language: {r['language'] or 'unknown'}\n"
        f"Title: {r['title'] or '(no title)'}\n"
        f"Text: {r['text']}"
        for i, r in enumerate(reviews)
    )
    return f"Analyze these {len(reviews)} reviews:\n\n{review}"

def enrich_batch(reviews: list[dict]) -> list[dict]:
    resp = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=SYSTEM_PROMPT + "\n\n" + build_user_prompt(reviews)
    )
    text = resp.text.strip()
    text = re.sub(r"```json|```", "", text).strip()
    return json.loads(text)

def get_reviews() -> list[dict]:
    query = f"""
        SELECT domain, date_published, reviewer_name, title, text, star_rating, language
        FROM `{table_ref}`
        WHERE sentiment is NULL
            AND has_text = TRUE
            AND text IS NOT NULL
            AND TRIM(text) != ''
            AND domain IS NOT NULL
            AND date_published IS NOT NULL
        ORDER BY date_published DESC
    """
    rows = bq.query(query).result()
    return [
        {
            "domain": str(row["domain"]),
            "date_published": row["date_published"].isoformat(),
            "reviewer_name": str(row["reviewer_name"]) if row["reviewer_name"] else None,
            "title": str(row["title"]) if row["title"] else None,
            "text": str(row["text"]),
            "star_rating": int(row["star_rating"]),
            "language": str(row["language"]) if row["language"] else None
        }
        for row in rows
    ]

def update_table(rows: list[dict]) -> None:
    if not rows:
        return
    
    temp = f"{PROJECT_ID}.{DATASET_ID}.enrichment_temp"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema =[
            bigquery.SchemaField("domain", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date_published", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("reviewer_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("sentiment", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("pain_point", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("insight", "STRING", mode="NULLABLE"),
        ]
    )

    job = bq.load_table_from_json(rows, temp, job_config=job_config)
    job.result()

    merge_query = f"""
        MERGE `{table_ref}` AS target
        USING (
            SELECT * 
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY domain, date_published, reviewer_name
                        ORDER BY domain
                    ) AS row_num
                FROM `{temp}`
            )
            WHERE row_num = 1
        ) AS source
        ON target.domain = source.domain
        AND target.date_published = source.date_published
        AND target.reviewer_name = source.reviewer_name
        WHEN MATCHED AND target.sentiment IS NULL THEN
            UPDATE SET
                target.sentiment = source.sentiment,
                target.category = source.category,
                target.pain_point = source.pain_point,
                target.insight = source.insight
    """
    bq.query(merge_query).result()
    print(f"Successfully enriched {len(rows)} reviews")

def run_enrichment() -> None:
    reviews = get_reviews()

    for i in range(0, len(reviews), BATCH_SIZE):
        batch = reviews[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(reviews) + BATCH_SIZE - 1) // BATCH_SIZE

        try:
            enrichments = enrich_batch(batch)
            enriched_rows = [
                {
                    "domain": batch[j]["domain"],
                    "date_published": batch[j]["date_published"],
                    "reviewer_name": batch[j]["reviewer_name"],
                    "sentiment": enrichments[j].get("sentiment"),
                    "category": enrichments[j].get("category"),
                    "pain_point": enrichments[j].get("pain_point"),
                    "insight": enrichments[j].get("insight"),
                } for j in range(len(batch))
            ]
            update_table(enriched_rows)
        except (json.JSONDecodeError, IndexError) as e:
            print(f"Batch {batch_num} failed - {e}")
            continue
        time.sleep(45)

if __name__ == "__main__":
    run_enrichment()