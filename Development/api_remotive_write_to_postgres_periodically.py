import requests
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
import schedule
import time

# Adjust this with your actual Postgres database credentials
DATABASE_URI = 'postgresql+psycopg2://postgres:Cmorikas77@localhost:5433/JobApplicationSystem'

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

def setup_database():
    # Use PostgreSQL database URI
    engine = create_engine(DATABASE_URI)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)

Session = setup_database()

def fetch_jobs():
    url = "https://remotive.io/api/remote-jobs"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['jobs']
    else:
        print(f"Failed to fetch jobs, status code: {response.status_code}")
        return None

def store_jobs(jobs):
    session = Session()
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

# Schedule the job fetching periodically
def job_runner():
    jobs = fetch_jobs()
    if jobs:
        store_jobs(jobs)

# For scheduling every X minutes
schedule.every(30).minutes.do(job_runner)  # Change frequency as needed

if __name__ == "__main__":
    # Run once immediately
    job_runner()
    
    # Keep running the scheduled task
    while True:
        schedule.run_pending()
        time.sleep(1)  # Wait 1 second between checks
