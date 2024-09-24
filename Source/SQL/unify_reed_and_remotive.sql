CREATE TABLE unified_jobs AS (
    SELECT 
        id,  -- id is already VARCHAR in job_ads_reed
        job_title AS title,  -- renaming job_title to title
        company_name,  -- company_name is common
        job_url AS url,  -- renaming job_url to url
        job_type,  -- job_type is common
        region,  -- region is common
        salary,  -- salary from reed
        NULL::VARCHAR AS category,  -- category doesn't exist in reed, so we set it to NULL
        NULL::TEXT AS tags,  -- tags don't exist in reed, so we set them to NULL
        timestamp AS date_posted  -- timestamp is already a TIMESTAMP in reed
    FROM job_ads_reed
    
    UNION ALL
    
    SELECT 
        id::VARCHAR,  -- cast id from serial4 (INTEGER) to VARCHAR
        title,  -- title from remotive
        company_name,  -- company_name is common
        url,  -- url from remotive
        job_type,  -- job_type is common
        region,  -- region is common
        NULL::FLOAT8 AS salary,  -- salary doesn't exist in remotive, so we set it to NULL with correct type
        category,  -- category from remotive
        tags,  -- tags from remotive
        TO_TIMESTAMP(date_posted, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS date_posted  -- Convert date_posted (VARCHAR) from remotive to TIMESTAMP
    FROM job_ads_remotive
);
