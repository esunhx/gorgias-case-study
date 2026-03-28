-- DATA SANITY REPORTS & FIXES
SELECT
    COUNT(*) AS total_rows,

    COUNTIF(domain IS NULL) AS null_domains,

    COUNTIF(star_rating IS NULL) AS null_ratings,
    COUNTIF(star_rating NOT BETWEEN 1 AND 5) AS out_of_range,
    COUNTIF(star_rating < 1) AS rating_b1,
    COUNTIF(star_rating > 5) AS rating_a5,
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
    )) AS double_candidates,
FROM `gorgias-case-study-491217.lead_enrichment.reviews`;

UPDATE `gorgias-case-study-491217.lead_enrichment.reviews`
SET star_rating = CASE
    WHEN star_rating < 1 THEN 1
    WHEN star_rating > 5 THEN 5
END
WHERE star_rating NOT BETWEEN 1 AND 5
    AND star_rating NOT NULL;

UPDATE `gorgias-case-study-491217.lead_enrichment.reviews`
SET date_published = NULL
WHERE date_published < TIMESTAMP('2010-01-01');

UPDATE `gorgias-case-study-491217.lead_enrichment.reviews`
SET has_text = (text IS NOT NULL AND TRIM(text) != '')
WHERE has_text != (text IS NOT NULL AND TRIM(text) !='');

DELETE FROM `gorgias-case-study-491217.lead_enrichment.reviews`
WHERE domain IS NULL
    AND date_published IS NULL
    AND star_rating IS NULL
    AND text IS NULL
    AND title IS NULL;

SELECT
    COUNT(*) as total_rows,
    COUNTIF(star_rating IS NULL) AS null_ratings,
    COUNTIF(star_rating NOT BETWEEN 1 AND 5) AS out_of_range,
    COUNTIF(date_published IS NULL) AS null_dates,
    COUNTIF(date_published > CURRENT_TIMESTAMP()) AS future_dates,
    COUNTIF(date_published < TIMESTAMP('2010-01-01')) AS past_dates,
    COUNTIF(has_text != (text IS NOT NULL AND TRIM(text) != '')) as mismatch,
    COUNT(*) - COUNT(DISTINCT CONCAT(
        COALESCE(domain, ''),
        CAST(date_published AS STRING),
        COALESCE(reviewer_name, '')
    )) AS double_candidates
FROM `gorgias-case-study-491217.lead_enrichment.reviews`
    
-- TOTAL REVIEWS QUERY
SELECT
    domain,
    COUNT(*) AS total_reviews,
    COUNT(star_rating) AS reviews_with_rating,
    COUNTIF(star_rating IS NULL) AS missing_ratings,
    ROUND(AVG(star_rating), 2) AS avg_rating,
    COUNTIF(has_text AND text is NOT NULL) AS reviews_with_text,
    COUNTIF(NOT has_text OR text is NULL) AS reviews_without_text,
    COUNTIF(company_replied) AS company_replies
FROM `gorgias-case-study-491217.lead_enrichment.reviews`
WHERE domain IS NOT NULL
GROUP BY 
HAVING COUNT(star_rating) > 0
ORDER BY total_reviews DESC;

-- STAR RATING PERCENTAGE
SELECT
    domain,
    star_rating,
    COUNT(*) AS count,
    ROUND(
        COUNT(*) * 100 / SUM(COUNT(*)) OVER (PARTITION by domain), 1
    ) AS percentage_per_domain
FROM `gorgias-case-study-491217.lead_enrichment.reviews`
WHERE domain IS NOT NULL
    AND star_rating IS NOT NULL
    AND star_rating BETWEEN 1 AND 5
GROUP BY domain, star_rating
ORDER BY domain, star_rating DESC;

-- LANGUAGE
SELECT 
    COALESCE(language, 'unknown') AS language,
    COUNT(*) AS count,
    ROUND(
        COUNT(*) * 100 / SUM(COUNT(*)) OVER(), 1
    ) AS percentage_of_total
FROM `gorgias-case-study-491217.lead_enrichment.reviews`
WHERE has_text = TRUE
    AND text is NOT NULL
GROUP BY language
ORDER BY count DESC;    

-- REVIEWS PER MONTH
SELECT
    domain,
    FORMAT_TIMESTAMP('Y%-m%', date_published) AS month,
    COUNT(*) AS total_reviews,
    COUNT(star_rating) AS reviews_with_rating,
    ROUND(AVG(star_rating), 2) AS avg_rating,
    COUNTIF(star_rating IS NULL) AS missing_ratings
FROM `gorgias-case-study-491217.lead_enrichment.reviews`
WHERE domain IS NOT NULL
    AND date_published IS NOT NULL
    AND date_published <= CURRENT_TIMESTAMP()
    AND date_published >= TIMESTAMP('2015-01-01')
GROUP BY domain, month
HAVING COUNT(*) > 0
ORDER BY domain, month DESC;