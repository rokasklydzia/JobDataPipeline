from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

# Airflow default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define your DAG
dag = DAG(
    '1_1_remotive_job_ads_dag',
    default_args=default_args,
    description='Fetch and store job ads from Remotive API',
    schedule_interval=timedelta(minutes=30),  # Set the schedule (runs every 30 minutes)
    start_date=datetime(2024, 1, 1),
    catchup=False
)

# SQLAlchemy setup
DATABASE_URI = 'postgresql+psycopg2://postgres:Cmorikas77@192.168.1.164:5433/JobApplicationSystem'
Base = declarative_base()

class JobAd(Base):
    __tablename__ = 'job_ads_remotive'
    id = Column(Integer, primary_key=True)
    title = Column(String)
    company_name = Column(String)
    job_type = Column(String)
    category = Column(String)
    url = Column(String)
    tags = Column(Text)
    date_posted = Column(String)
    region = Column(String)  # New column for job region

# Set up the database and session
def setup_database():
    engine = create_engine(DATABASE_URI)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)

Session = setup_database()

# Function to fetch jobs from Remotive API
def fetch_jobs(**kwargs):
    url = "https://remotive.io/api/remote-jobs"
    response = requests.get(url)
    if response.status_code == 200:
        jobs = response.json()['jobs']
        return jobs
    else:
        print(f"Failed to fetch jobs, status code: {response.status_code}")
        return []

# Function to store jobs in the database
def store_jobs(**kwargs):
    jobs = kwargs['ti'].xcom_pull(task_ids='fetch_jobs_task')
    session = Session()
    
    if jobs:
        for job in jobs:
            # Check if job already exists by ID
            existing_job = session.query(JobAd).filter_by(id=job['id']).first()
            
            # Only add the job if it doesn't exist
            if existing_job is None:
                try:
                    job_ad = JobAd(
                        id=job['id'],
                        title=job['title'],
                        company_name=job['company_name'],
                        job_type=job['job_type'],
                        category=job['category'],
                        url=job['url'],
                        tags=",".join(job['tags']),
                        date_posted=job['publication_date'],
                        region=job.get('candidate_required_location', 'Unknown')  # Extracting region
                    )
                    session.add(job_ad)
                except IntegrityError:
                    session.rollback()  # Rollback in case of integrity error
            else:
                print(f"Job {job['id']} already exists in the database, skipping.")

        session.commit()
        print(f"Stored {len(jobs)} jobs")

# Define the PythonOperator tasks for the DAG
fetch_jobs_task = PythonOperator(
    task_id='fetch_jobs_task',
    python_callable=fetch_jobs,
    dag=dag,
)

store_jobs_task = PythonOperator(
    task_id='store_jobs_task',
    python_callable=store_jobs,
    provide_context=True,  # This allows access to Airflow's XCom system
    dag=dag,
)

# Define the task dependencies
fetch_jobs_task >> store_jobs_task
