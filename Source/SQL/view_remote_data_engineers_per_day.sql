CREATE VIEW data_engineer_remote_job_count_per_day AS
SELECT 
    DATE(date_posted) AS posting_date,
    COUNT(*) AS job_count
FROM 
    unified_jobs
WHERE 
    LOWER(title) LIKE '%data engineer%'  -- case-insensitive search for "data engineer"
    AND LOWER(region) LIKE '%remote%'  -- filter for remote-friendly jobs
GROUP BY 
    DATE(date_posted)
ORDER BY 
    posting_date;
