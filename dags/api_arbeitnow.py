from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from sqlalchemy import create_engine, Column, Integer, String, Text, Boolean, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

## Use dotenv for safe password and other string use
#from dotenv import load_dotenv
#import os
#load_dotenv()
#database_url = os.getenv("DATABASE_URL")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    '1_3_arbeitnow_job_ads_dag',
    default_args=default_args,
    description='Fetch and store job ads from Arbeitnow API',
    schedule_interval=timedelta(hours=1),  # Schedule the DAG to run every hour
    start_date=datetime(2024, 1, 1),
    catchup=False
)

# Database setup
#DATABASE_URL = database_url #Load database_url from dotenv
DATABASE_URL = "postgresql://postgres:Cmorikas77@192.168.1.164:5433/JobApplicationSystem"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Define the table for storing job ads from Arbeitnow
class ArbeitnowJobAd(Base):
    __tablename__ = 'job_ads_arbeitnow'
    id = Column(Integer, primary_key=True, autoincrement=True)
    slug = Column(String, unique=True, nullable=False)
    company_name = Column(String, nullable=False)
    title = Column(String, nullable=False)
    description = Column(Text)
    category = Column(String)
    tags = Column(Text)  # Store as a comma-separated string for simplicity
    job_type = Column(String)
    location = Column(String)
    remote = Column(Boolean)
    url = Column(String, nullable=False)
    date_posted = Column(Date)
    salary = Column(String)

# Create the table in the database (if it doesn't exist)
Base.metadata.create_all(engine)

# Function to fetch jobs from Arbeitnow API
def fetch_jobs_from_arbeitnow(**kwargs):
    all_jobs = []
    page = 1
    while True:
        url = f'https://www.arbeitnow.com/api/job-board-api?page={page}'
        response = requests.get(url)
        if response.status_code == 200:
            jobs = response.json().get('data', [])
            if not jobs:
                break  # No more jobs
            all_jobs.extend(jobs)
            page += 1
        else:
            raise Exception(f"Failed to fetch jobs, status code: {response.status_code}")
    return all_jobs

# Function to store jobs into the database
def store_jobs(**kwargs):
    jobs = kwargs['ti'].xcom_pull(task_ids='fetch_jobs_task')
    session = Session()
    new_jobs_count = 0
    for job in jobs:
        existing_job = session.query(ArbeitnowJobAd).filter_by(slug=job.get('slug')).first()
        if not existing_job:
            job_ad = ArbeitnowJobAd(
                slug=job.get('slug'),
                company_name=job.get('company_name'),
                title=job.get('title'),
                description=job.get('description'),
                category=job.get('category'),
                tags=",".join(job.get('tags', [])),  # Join tags into a comma-separated string
                job_type=job.get('job_type'),
                location=job.get('location'),
                remote=job.get('remote'),
                url=job.get('url'),
                date_posted=datetime.strptime(job.get('date_posted'), '%Y-%m-%d') if job.get('date_posted') else None,
                salary=job.get('salary')
            )
            session.add(job_ad)
            new_jobs_count += 1
    session.commit()
    print(f"Stored {new_jobs_count} new jobs")

# Define tasks
fetch_jobs_task = PythonOperator(
    task_id='fetch_jobs_task',
    python_callable=fetch_jobs_from_arbeitnow,
    dag=dag,
)

store_jobs_task = PythonOperator(
    task_id='store_jobs_task',
    python_callable=store_jobs,
    provide_context=True,
    dag=dag,
)

# Task dependencies
fetch_jobs_task >> store_jobs_task
