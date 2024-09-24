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
    __tablename__ = 'job_ads'

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

