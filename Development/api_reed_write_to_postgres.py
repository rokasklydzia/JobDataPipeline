import requests
from requests.auth import HTTPBasicAuth

category = 'data engineer'
API_KEY = 'c6911d48-f582-4e0a-bd16-ae054a0ed51b'
API_URL = f'https://www.reed.co.uk/api/1.0/search?keywords={category}&location=london&distancefromlocation=15'

# Define API parameters
params = {
    'keywords': 'data engineer, analytics engineer',
    'location': 'remote',
    'resultsToTake': 1000  # Adjust based on how many ads you want to fetch (max 100)
}

# Make the API request with Basic Authentication
response = requests.get(API_URL, params=params, auth=HTTPBasicAuth(API_KEY, ''))

# Check if request was successful
if response.status_code == 200:
    data = response.json()
    print(f"Fetched {len(data['results'])} job ads.")
else:
    print(f"Failed to fetch data: {response.status_code}, {response.text}")

data.dtype

type(data['results'])
type(data['results'][0])

from sqlalchemy import create_engine, Column, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Database setup
DATABASE_URL = "postgresql://postgres:Cmorikas77@localhost:5433/JobApplicationSystem"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Declare a mapping
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

# Create the table
Base.metadata.create_all(engine)

def map_api_data_to_job_ad(api_data):
    # Helper function to convert API data to SQLAlchemy model
    salary = None
    if api_data['minimumSalary'] and api_data['maximumSalary']:
        salary = (api_data['minimumSalary'] + api_data['maximumSalary']) / 2
    
    return JobAd(
        id=str(api_data['jobId']),
        job_title=api_data['jobTitle'],
        company_name=api_data['employerName'],
        job_url=api_data['jobUrl'],
        job_type='Full-time',  # Or extract this info from jobDescription
        region='Europe',  # Or extract region info from locationName
        salary=salary,
        timestamp=datetime.strptime(api_data['date'], '%d/%m/%Y')
    )


# Assuming you have fetched data from API in 'data' object
job_ads = data['results']

# Convert each job ad to a SQLAlchemy model instance
mapped_job_ads = [map_api_data_to_job_ad(job) for job in job_ads]

# Add all job ads to the session and commit to the database
session.bulk_save_objects(mapped_job_ads)
session.commit()

print(f"Successfully inserted {len(mapped_job_ads)} job ads into the database.")
