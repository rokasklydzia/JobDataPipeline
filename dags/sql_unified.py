from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

DATABASE_URI = 'postgresql+psycopg2://postgres:Cmorikas77@192.168.1.164:5433/JobApplicationSystem'

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 1),
    'retries': 1,
}

# Initialize the DAG
with DAG(
    dag_id='2_1_create_unified_jobs_table',
    default_args=default_args,
    schedule_interval=None,  # You can adjust the schedule if needed
    catchup=False
) as dag:

    # SQL command to create the unified_jobs table
    create_unified_jobs_table = PostgresOperator(
        task_id='create_unified_jobs',
        postgres_conn_id='PostgresDesktop',  # Update with the connection ID you configured
        sql="""
        DROP VIEW IF EXISTS data_engineer_job_count_per_day;
        DROP VIEW IF EXISTS data_engineer_remote_job_count_per_day;
        DROP VIEW IF EXISTS data_engineer_salary_stats;

        DROP TABLE IF EXISTS unified_jobs;

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
        """
    )

    create_unified_jobs_table
