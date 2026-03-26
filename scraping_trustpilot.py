from curl_cffi import requests as cffi_requests
import re
import json
import time

def get_next_data(session, url: str) -> dict:
    resp = session.get(url)
    if resp.status_code == 404:
        return {}

    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', resp.text, re.DOTALL)
    if not match:
        return {}
        
    return json.loads(match.group(1))

def parse_review(review: dict, company_domain: str) -> dict:
    return {
        "domain":          company_domain,
        "title":           review.get("title"),
        "text":            review.get("text"),
        "star_rating":     review.get("rating"),
        "date_published":  review.get("dates", {}).get("publishedDate"),
        "reviewer_name":   review.get("consumer", {}).get("displayName"),
        "company_replied": review.get("reply") is not None,
    }

def scrape_trustpilot_reviews(company_domain: str) -> list[dict]:
    session = cffi_requests.Session(impersonate="chrome120")
    session.headers.update({
        "Referer": f"https://www.trustpilot.com/review/{company_domain}",
        "Accept": "application/json, */*",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "same-origin",
        "Sec-Fetch-Site": "same-origin",
    })

    all_reviews = []
    page_number = 1

    while True:
        url=f"https://www.trustpilot.com/review/{company_domain}?page={page_number}"
        print(f"Searching page: {page_number}")
        data = get_next_data(session, url)

        if not data:
            print("No reviews to fecth at this url")
            break

        page_reviews = data["props"]["pageProps"].get("reviews", [])

        all_reviews.extend(parse_review(r, company_domain) for r in page_reviews)
        page_number += 1
        time.sleep(2.5)
    
    return all_reviews

reviews = scrape_trustpilot_reviews("gorgias.com")
print(len(reviews))