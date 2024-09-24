from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

# Import your Python functions here
from Development.api_remotive_write_to_postgres_periodically import fetch_jobs as fetch_remotive_jobs, store_jobs as store_remotive_jobs
from Development.api_reed_write_to_postgres import map_api_data_to_job_ad, mapped_job_ads, store_jobs as store_reed_jobs
from Development.api_arbeitnow_write_to_postgres_periodically import fetch_jobs_from_arbeitnow, store_jobs as store_arbeitnow_jobs

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Initialize the DAG
with DAG('job_application_system_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    # Python tasks for API scripts
    def fetch_and_store_remotive_jobs():
        jobs = fetch_remotive_jobs()
        store_remotive_jobs(jobs)

    task_fetch_remotive_jobs = PythonOperator(
        task_id='fetch_and_store_remotive_jobs',
        python_callable=fetch_and_store_remotive_jobs
    )

    def fetch_and_store_reed_jobs():
        # Your logic to fetch Reed API jobs and store them
        store_reed_jobs(mapped_job_ads)

    task_fetch_reed_jobs = PythonOperator(
        task_id='fetch_and_store_reed_jobs',
        python_callable=fetch_and_store_reed_jobs
    )

    def fetch_and_store_arbeitnow_jobs():
        jobs = fetch_jobs_from_arbeitnow()
        store_arbeitnow_jobs(jobs)

    task_fetch_arbeitnow_jobs = PythonOperator(
        task_id='fetch_and_store_arbeitnow_jobs',
        python_callable=fetch_and_store_arbeitnow_jobs
    )

    # SQL task to unify Reed and Remotive data
    task_unify_jobs = PostgresOperator(
        task_id='unify_jobs',
        postgres_conn_id='your_postgres_conn_id',
        sql='sql/unify_reed_remotive.sql'  # Store the SQL script in a file
    )

    # SQL task to join Arbeitnow with unified jobs
    task_join_arbeitnow = PostgresOperator(
        task_id='join_arbeitnow_jobs',
        postgres_conn_id='your_postgres_conn_id',
        sql='sql/join_unified_and_arbeitnow.sql'  # Store the SQL script in a file
    )

    # SQL tasks to create views
    task_create_data_engineer_view = PostgresOperator(
        task_id='create_data_engineer_view',
        postgres_conn_id='your_postgres_conn_id',
        sql='sql/create_data_engineer_view.sql'  # SQL script to create the view
    )

    task_create_remote_engineer_view = PostgresOperator(
        task_id='create_remote_engineer_view',
        postgres_conn_id='your_postgres_conn_id',
        sql='sql/create_remote_engineer_view.sql'  # SQL script to create the view
    )

    task_create_salary_stats_view = PostgresOperator(
        task_id='create_salary_stats_view',
        postgres_conn_id='your_postgres_conn_id',
        sql='sql/create_salary_stats_view.sql'  # SQL script to create the view
    )

    # Set task dependencies
    [task_fetch_remotive_jobs, task_fetch_reed_jobs, task_fetch_arbeitnow_jobs] >> task_unify_jobs >> task_join_arbeitnow
    task_join_arbeitnow >> [task_create_data_engineer_view, task_create_remote_engineer_view, task_create_salary_stats_view]
