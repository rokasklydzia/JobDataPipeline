from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

# Default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    '2_2_insert_arbeitnow_jobs_into_unified_jobs',
    default_args=default_args,
    description='Insert new job ads from arbeitnow_job_ads into unified_jobs',
    schedule_interval=timedelta(days=1),  # Set it to run daily, adjust as needed
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['arbeitnow', 'job-ads']
) as dag:

    # Define the PostgresOperator to execute the SQL script
    insert_jobs_task = PostgresOperator(
        task_id='insert_arbeitnow_jobs',
        postgres_conn_id='PostgresDesktop',  # Make sure your connection ID is set up in Airflow
        sql="""
        -- Insert new job ads from arbeitnow_job_ads into unified_jobs table
        INSERT INTO unified_jobs (
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
            a.title AS job_title,    -- Renaming 'title' to 'job_title'
            a.company_name, 
            a.url AS url,        -- Renaming 'url' to 'job_url'
            a.job_type, 
            a.location AS region,    -- Renaming 'location' to 'region'
            NULLIF(a.salary, '')::DOUBLE PRECISION,  -- Cast 'salary' to double precision and handle empty strings
            a.date_posted AS date_posted,  -- Renaming 'date_posted' to 'timestamp'
            a.category, 
            a.tags 
        FROM job_ads_arbeitnow a
        LEFT JOIN unified_jobs u 
        ON a.url = u.url    -- Prevent duplicate entries based on job_url
        WHERE u.url IS NULL;  -- Only insert jobs that are not already in unified_jobs
        """
    )

# Set task dependencies (if you have more tasks)
insert_jobs_task
