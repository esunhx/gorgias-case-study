from curl_cffi import requests as cffi_requests
from google.cloud import bigquery
from data.cleaning  import parse_review

import re
import json
import time

PROJECT_ID = "gorgias-case-study-491217"
DATASET_ID = "lead_enrichment"
TABLE_ID = "reviews"

bq = bigquery.Client(project=PROJECT_ID)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

def get_domains() -> list[str]:
    query = f"""
        SELECT DISTINCT lead.domain
        FROM `{PROJECT_ID}.{DATASET_ID}.leads` lead
        LEFT JOIN (
            SELECT domain, MAX(scraped_at) AS last_scraped
            FROM `{table_ref}`
            GROUP BY domain
        ) rev ON lead.domain = rev.domain
        WHERE rev.last_scraped is NULL
            OR rev.last_scraped < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        ORDER BY lead.domain
    """
    rows = bq.query(query).result()
    return [row['domain'] for row in rows]

def get_next_data(session, url: str) -> dict:
    resp = session.get(url)
    if resp.status_code == 404:
        return {}

    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', resp.text, re.DOTALL)
    if not match:
        return {}
        
    return json.loads(match.group(1))

def insert_to_bq(rows: list[dict]) -> None:
    if not rows:
        return

    temp_table = f"{PROJECT_ID}.{DATASET_ID}.temp"
    job_config = bq.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema = [
            bigquery.SchemaField("domain", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("star_rating", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("date_published", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("reviewer_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("comapny_replied", "BOOL", mode="REQUIRED"),
            bigquery.SchemaField("language", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("has_text", "BOOL", mode="REQUIRED"),
            bigquery.SchemaField("scraped_at", "TIMESTAMP", mode="REQUIRED")
        ]
    )
    
    job = bq.load_table_from_json(rows, temp_table, job_config=job_config)
    job.result()

    merge_query = f"""
        MERGE `{table_ref}` AS target
        USING `{temp_table}` AS source
        ON target.domain = source.domain
        AND target.date_published = source.date_published
        AND target.reviewer_name = source.reviewer_name
        WHEN NOT MATCHED THEN
            INSERT (
                domain, title, text, star_rating, date_published, reviewer_name,
                complany_replied, language, has_text, scraped_at
            )
            VALUES (
                source.domain, source.title, source.text, source.star_rating, source.date_published,
                source.reviewer_name, soure.complany_replied, source.language, source.has_text, 
                source.scraped_at
            )
    """
    bq.query(merge_query).result()

def scrape_and_store(company_domain: str) -> None:
    session = cffi_requests.Session(impersonate="chrome120")
    session.headers.update({
        "Referer": f"https://www.trustpilot.com/review/{company_domain}",
        "Accept": "application/json, */*",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "same-origin",
        "Sec-Fetch-Site": "same-origin",
    })

    page_number = 1
    total_inserted = 0
    total_skipped = 0

    while True:
        url=f"https://www.trustpilot.com/review/{company_domain}?page={page_number}"
        print(f"Searching page: {page_number}")
        data = get_next_data(session, url)

        if not data:
            print("No reviews to fecth at this url")
            break

        reviews = data["props"]["pageProps"].get("reviews", [])
        parsed = [parse_review(r, company_domain) for r in reviews]
        clean = [r for r in parsed if r is not None]
        skipped = len(parsed) - len(clean)

        if clean:
            try:
                insert_to_bq(clean)
            except RuntimeError as e:
                print(f"Skipping page {page_number} - {e}")
                continue
            total_inserted += 1
        
        total_skipped += skipped
        page_number += 1
        time.sleep(2.5)
    
    print(
        f"For {domain}, scraped {page_number} pages, inserted {total_inserted} and skipped {total_skipped}"
    )

if __name__ == "__main__":
    domains = get_domains()
    for domain in domains:
        print(f"Scraping {domain}")
        scrape_and_store(domain)
