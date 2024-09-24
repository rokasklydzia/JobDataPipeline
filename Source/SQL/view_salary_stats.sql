CREATE VIEW data_engineer_salary_stats AS
SELECT 
    DATE(date_posted) AS posting_date,
    COUNT(*) AS job_count,
    MAX(salary) AS max_salary,
    MIN(salary) AS min_salary,
    AVG(salary) AS avg_salary,
    STDDEV(salary) AS salary_stddev
FROM 
    unified_jobs
WHERE 
    LOWER(title) LIKE '%data engineer%'  -- case-insensitive search for "data engineer"
    AND salary IS NOT NULL  -- exclude jobs where salary is missing
GROUP BY 
    DATE(date_posted)
ORDER BY 
    posting_date;
