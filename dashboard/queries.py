from google.cloud import bigquery

bq = bigquery.Client(project="gorgias-case-study-491217")

def get_overview() -> list[dict]:
    query = """
        SELECT
            r.domain,
            l.company_name,
            COUNT(*) AS total_reviews,
            ROUND(AVG(r.star_rating), 2) AS avg_rating,
            ROUND(COUNTIF(r.sentiment = 'positive') * 100 / COUNT(*), 1) AS positive_ratio,
            ROUND(COUNTIF(r.sentiment = 'negative') * 100 / COUNT(*), 1) AS negative_ratio,
            ROUND(COUNTIF(r.sentiment = 'neutral') * 100 / COUNT(*), 1) AS neutral_ratio,
            COUNTIF(r.company_replied) AS total_replies,
            ROUND(COUNTIF(r.company_replied) * 100 / COUNT(*), 1) AS reply_ratio
        FROM `gorgias-case-study-491217.lead_enrichment.reviews` r
        LEFT JOIN `gorgias-case-study-491217.lead_enrichment.leads` l
            ON r.domain = l.domain
        WHERE r.domain IS NOT NULL
        GROUP BY r.domain, l.company_name
        ORDER BY total_reviews DESC
    """
    return [dict(row) for row in bq.query(query).result()]

def get_category(domain: str) -> list[dict]:
    query = f"""
        SELECT
            category,
            COUNT(*) AS count,
            ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER (), 1) AS ratio,
        FROM `gorgias-case-study-491217.lead_enrichment.reviews`
        WHERE domain = '{domain}'
            AND category IS NOT NULL
        GROUP BY category
        ORDER BY count DESC
    """
    return [dict(row) for row in bq.query(query).result()]

def get_pain_points(domain: str) -> list[dict]:
    query = f"""
        SELECT
            pain_point,
            insight,
            star_rating,
            date_published,
            reviewer_name
        FROM `gorgias-case-study-491217.lead_enrichment.reviews`
        WHERE domain = '{domain}'
            AND sentiment = 'negative'
            AND pain_point IS NOT NULL
        ORDER BY date_published DESC
        LIMIT 10
    """
    return [dict(row) for row in bq.query(query).result()]

def get_reviews(domain: str) -> list[dict]:
    query = f"""
        SELECT
            reviewer_name,
            star_rating,
            date_published,
            title,
            text,
            sentiment,
            category,
            pain_point,
            insight,
            company_replied,
            language
        FROM `gorgias-case-study-491217.lead_enrichment.reviews`
        WHERE domain = '{domain}'
            AND date_published IS NOT NULL
        ORDER BY date_published DESC
    """
    return [dict(row) for row in bq.query(query).result()]