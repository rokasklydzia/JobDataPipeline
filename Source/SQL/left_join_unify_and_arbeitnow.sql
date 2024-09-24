-- Insert new job ads from arbeitnow_job_ads into unified_jobs table
INSERT INTO unified_jobs (
    --source, 
    --id,
    title, 
    company_name, 
    url, 
    job_type, 
    region, 
    salary, 
    date_posted, 
    category, 
    tags
)
SELECT 
    --'arbeitnow' AS source,  -- Adding the source field
    a.title AS job_title,    -- Renaming 'title' to 'job_title'
    a.company_name, 
    a.url AS url,        -- Renaming 'url' to 'job_url'
    a.job_type, 
    a.location AS region,    -- Renaming 'location' to 'region'
    NULLIF(a.salary, '')::DOUBLE PRECISION,  -- Cast 'salary' to double precision and handle empty strings
    a.date_posted AS date_posted,  -- Renaming 'date_posted' to 'timestamp'
    a.category, 
    a.tags 
   -- a.remote AS remote  -- Boolean for remote-friendly jobs
FROM job_ads_arbeitnow a
LEFT JOIN unified_jobs u 
ON a.url = u.url    -- Prevent duplicate entries based on job_url
WHERE u.url IS NULL;  -- Only insert jobs that are not already in unified_jobs
