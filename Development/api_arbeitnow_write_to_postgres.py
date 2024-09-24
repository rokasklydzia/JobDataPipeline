import requests
from sqlalchemy import create_engine, Column, Integer, String, Text, Boolean, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Database setup
DATABASE_URL = "postgresql://postgres:Cmorikas77@localhost:5433/JobApplicationSystem"  # Update this with your credentials
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
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

# Function to fetch jobs from Arbeitnow with pagination
def fetch_jobs_from_arbeitnow():
    all_jobs = []
    page = 1
    while True:
        url = f'https://www.arbeitnow.com/api/job-board-api?page={page}'
        response = requests.get(url)
        if response.status_code == 200:
            jobs = response.json().get('data', [])
            if not jobs:  # No more jobs on the next page
                break
            all_jobs.extend(jobs)
            page += 1
        else:
            print(f"Failed to fetch jobs, status code: {response.status_code}")
            break
    return all_jobs

# Function to store job ads into the database, appending only new ones
def store_jobs(jobs):
    session = Session()
    new_jobs_count = 0
    for job in jobs:
        # Check if the job ad already exists based on the unique 'slug' field
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

# Main script to fetch and store jobs
if __name__ == "__main__":
    jobs = fetch_jobs_from_arbeitnow()
    if jobs:
        store_jobs(jobs)
