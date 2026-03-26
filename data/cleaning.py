from langdetect import detect, LangDetectException
from datetime import datetime, timezone

def detect_language(text: str) -> str | None:
    try:
        return detect(text) if text and len(text) > 20 else None
    except LangDetectException:
        return None

def clean_text(text: str | None) -> str | None:
    if not text:
        return None
    text = text.strip()
    return text if text else None

def parse_date(date_str: str | None) -> str | None:
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        return None

def parse_review(review: dict, company_domain: str) -> dict | None:
    rating = review.get("rating")
    if not isinstance(rating, int) or not (1 <= rating <= 5):
        return None

    date_published = parse_date(review.get("dates", {}).get("publishedDate"))
    if not date_published:
        return None

    text  = clean_text(review.get("text"))
    title = clean_text(review.get("title"))

    return {
        "domain":          company_domain,
        "title":           title,
        "text":            text,
        "star_rating":     rating,
        "date_published":  date_published,
        "reviewer_name":   clean_text(review.get("consumer", {}).get("displayName")),
        "company_replied": review.get("reply") is not None,
        "language":        detect_language(text),
        "has_text":        text is not None,
        "scraped_at":      datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }
