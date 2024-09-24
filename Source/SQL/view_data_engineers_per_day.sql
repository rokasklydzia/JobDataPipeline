CREATE VIEW data_engineer_job_count_per_day AS
SELECT 
    DATE(date_posted) AS posting_date,
    COUNT(*) AS job_count
FROM 
    unified_jobs
WHERE 
    LOWER(title) LIKE '%data engineer%'  -- case-insensitive search for "data engineer"
GROUP BY 
    DATE(date_posted)
ORDER BY 
    posting_date;
