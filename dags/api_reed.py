from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine, Column, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Default args for the DAG
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
    '1_2_reed_job_ads_dag',
    default_args=default_args,
    description='Fetch and store job ads from Reed API',
    schedule_interval=timedelta(hours=1),  # Runs every hour
    start_date=datetime(2024, 1, 1),
    catchup=False
)

# Database setup
DATABASE_URL = "postgresql://postgres:Cmorikas77@192.168.1.164:5433/JobApplicationSystem"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

class JobAd(Base):
    __tablename__ = 'job_ads_reed'

    id = Column(String, primary_key=True)
    job_title = Column(String, nullable=False)
    company_name = Column(String, nullable=False)
    job_url = Column(String, nullable=False)
    job_type = Column(String, nullable=True)
    region = Column(String, nullable=True)
    salary = Column(Float, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)

# Create the table if it doesn't exist
Base.metadata.create_all(engine)

# Helper function to map API data to SQLAlchemy model
def map_api_data_to_job_ad(api_data):
    salary = None
    if api_data['minimumSalary'] and api_data['maximumSalary']:
        salary = (api_data['minimumSalary'] + api_data['maximumSalary']) / 2
    
    return JobAd(
        id=str(api_data['jobId']),
        job_title=api_data['jobTitle'],
        company_name=api_data['employerName'],
        job_url=api_data['jobUrl'],
        job_type='Full-time',
        region='Europe',
        salary=salary,
        timestamp=datetime.strptime(api_data['date'], '%d/%m/%Y')
    )

# Function to fetch jobs from Reed API
def fetch_reed_jobs(**kwargs):
    API_KEY = 'c6911d48-f582-4e0a-bd16-ae054a0ed51b'
    API_URL = 'https://www.reed.co.uk/api/1.0/search'

    # Define API parameters
    params = {
        'keywords': 'data engineer, analytics engineer',
        'location': 'remote',
        'resultsToTake': 100  # Fetch up to 100 ads
    }

    # Make API request
    response = requests.get(API_URL, params=params, auth=HTTPBasicAuth(API_KEY, ''))
    if response.status_code == 200:
        return response.json()['results']
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

# Function to store jobs in the database
def store_reed_jobs(**kwargs):
    # Get the jobs fetched by the previous task
    job_ads = kwargs['ti'].xcom_pull(task_ids='fetch_reed_jobs_task')

    # Convert each job ad to SQLAlchemy model instance
    mapped_job_ads = [map_api_data_to_job_ad(job) for job in job_ads]

    # Store job ads in the database
    session.bulk_save_objects(mapped_job_ads)
    session.commit()

    print(f"Successfully inserted {len(mapped_job_ads)} job ads into the database.")

# Define the tasks in the DAG

# Task to fetch jobs from Reed API
fetch_reed_jobs_task = PythonOperator(
    task_id='fetch_reed_jobs_task',
    python_callable=fetch_reed_jobs,
    dag=dag,
)

# Task to store jobs in the PostgreSQL database
store_reed_jobs_task = PythonOperator(
    task_id='store_reed_jobs_task',
    python_callable=store_reed_jobs,
    provide_context=True,  # Allow access to XCom
    dag=dag,
)

# Task dependencies
fetch_reed_jobs_task >> store_reed_jobs_task
