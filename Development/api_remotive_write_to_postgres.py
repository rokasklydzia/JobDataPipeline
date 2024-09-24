import requests
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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
    session.commit()
    print(f"Stored {len(jobs)} jobs")

if __name__ == "__main__":
    jobs = fetch_jobs()
    if jobs:
        store_jobs(jobs)
