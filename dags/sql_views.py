from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Instantiate the DAG
with DAG(
    '3_create_data_engineer_views_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task 1: Create view for job count per day
    create_job_count_view = PostgresOperator(
        task_id='create_job_count_view',
        postgres_conn_id='PostgresDesktop',  # Use your defined connection ID
        sql="""
        CREATE VIEW data_engineer_job_count_per_day AS
        SELECT 
            DATE(date_posted) AS posting_date,
            COUNT(*) AS job_count
        FROM 
            unified_jobs
        WHERE 
            LOWER(title) LIKE '%data engineer%'
        GROUP BY 
            DATE(date_posted)
        ORDER BY 
            posting_date;
        """
    )

    # Task 2: Create view for remote job count per day
    create_remote_job_count_view = PostgresOperator(
        task_id='create_remote_job_count_view',
        postgres_conn_id='PostgresDesktop',  # Use your defined connection ID
        sql="""
        CREATE VIEW data_engineer_remote_job_count_per_day AS
        SELECT 
            DATE(date_posted) AS posting_date,
            COUNT(*) AS job_count
        FROM 
            unified_jobs
        WHERE 
            LOWER(title) LIKE '%data engineer%'
            AND LOWER(region) LIKE '%remote%'
        GROUP BY 
            DATE(date_posted)
        ORDER BY 
            posting_date;
        """
    )

    # Task 3: Create view for salary statistics
    create_salary_stats_view = PostgresOperator(
        task_id='create_salary_stats_view',
        postgres_conn_id='PostgresDesktop',  # Use your defined connection ID
        sql="""
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
            LOWER(title) LIKE '%data engineer%'
            AND salary IS NOT NULL
        GROUP BY 
            DATE(date_posted)
        ORDER BY 
            posting_date;
        """
    )

    # Define task dependencies
    create_job_count_view >> create_remote_job_count_view >> create_salary_stats_view
