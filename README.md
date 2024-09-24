# JobDataPipeline
Job Data Pipeline with Airflow and PostgreSQL

This project implements a data pipeline using Apache Airflow and PostgreSQL to process and analyze job postings from multiple sources. The pipeline extracts, transforms, and loads (ETL) job data into a PostgreSQL database and creates views to analyze job trends for "Data Engineer" positions.

## Table of Contents

- [Project Overview](#project-overview)
- [Technologies Used](#technologies-used)
- [Project Setup](#project-setup)
- [Airflow DAGs](#airflow-dags)
- [PostgreSQL Views](#postgresql-views)
- [How to Run the Pipeline](#how-to-run-the-pipeline)
- [Troubleshooting](#troubleshooting)
- [Future Enhancements](#future-enhancements)

## Project Overview

The data pipeline is designed to:

1. Collect job postings from external sources (e.g., APIs, websites).
2. Store the job postings in a unified PostgreSQL database.
3. Create views that generate insights on job trends for "Data Engineer" roles:
   - Count of data engineering jobs posted per day.
   - Count of remote data engineering jobs posted per day.
   - Salary statistics (min, max, average) for data engineering roles.

## Technologies Used

- **Apache Airflow**: Workflow orchestration to manage ETL tasks.
- **PostgreSQL**: Database to store and analyze job data.
- **SQLAlchemy**: Python SQL toolkit and ORM used for interacting with the database.
- **Docker**: To run Airflow in containers for easy setup and scalability.
- **BeautifulSoup** (optional): Web scraping library used for extracting job data from websites.

## Project Setup

### Prerequisites

- Docker and Docker Compose installed.
- PostgreSQL instance running (either locally or via Docker).
- Basic knowledge of SQL, Python, and Apache Airflow.

### Setting up the Project

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/TuringCollegeSubmissions/roklydz-DE2.3.5.git
   cd roklydz-DE2.3.5

2. **Start Docker Containers: Set up and start Airflow and PostgreSQL using Docker Compose**:
3. *Configure PostgreSQL Connection in Airflow*:
4. *Enable the DAG*:
## Airflow DAGs
The project consists of multiple DAGs, each responsible for specific tasks in the ETL process:

1. ETL DAG:

- Extracts job data from APIs and websites.
- Transforms the data and ensures it's in a uniform format.
- Loads the data into the PostgreSQL unified_jobs table.
2. View Creation DAG:

- Creates three views in PostgreSQL to analyze data engineering jobs:
1. data_engineer_job_count_per_day
2. data_engineer_remote_job_count_per_day
3. data_engineer_salary_stats

PostgreSQL Views

SQL Views are created using the combined data of multiple job ad sources. The ERD schema looks like this (no relational connections between the tables):
![image](https://github.com/user-attachments/assets/3bb54cac-824a-4352-a2aa-d38927590298)

1. Data Engineering Job Count Per Day

```sql
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
```
![image](https://github.com/user-attachments/assets/465ad850-72d9-41ff-a429-303c33738497)

2. Remote Data Engineering Job Count Per Day

```sql
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
```
3. Data Engineer Salary Statistics
```sql
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
```
![image](https://github.com/user-attachments/assets/1984877b-f062-462b-b090-2913f30aaeb3)

##How to Run the Pipeline
1. **Start Airflow**: Run the following command to start all services:

```bash
docker-compose up
```
Once the load completes, go to https://localhost:8080 to access the Airflow UI.
After logging in (the usual username is *airflow* and password is *airflow*) you should see a list of dags:
![image](https://github.com/user-attachments/assets/89fc2f03-bbd1-4519-ace7-3b09ebde2dc0)


2. **Trigger the DAGs**: In the Airflow UI, trigger the ETL DAG to load job data into the database. After the ETL DAG completes, trigger the view-creation DAG to generate views.

## Troubleshooting
- Connection Errors: Ensure that the PostgresDesktop connection in Airflow is correctly configured with the right database credentials.
- SQL Errors: Double-check the SQL queries for syntax errors or incorrect table/column names.
- Docker Issues: Restart Docker services using docker-compose down && docker-compose up.
- If you're using a local instance of Postgres and a Docker image of Airflow, like I am, you need to make sure the Postgres instance accepts connections from all sources. Locate your postgresql.conf (typically in /etc/postgresql/<version>/main/ on Linux). Ensure this line is present and uncommented:
```arduino
listen_addresses = '*'
```
- Check the pg_hba.conf file to make sure it allows local connections. Add the following line if it's not already present:
```css
host    all             all             127.0.0.1/32            
```
- If there is a problem with Airflow user group, make sure to comment out this line of code in the docker-compose.yaml file:
![image](https://github.com/user-attachments/assets/dea2236f-8fc5-43e3-aa7a-d5c015a4e0c6)
- If you don't want your Airflow DAGs cluttered with prebuilt DAG examples, set this variable to 'false':
![image](https://github.com/user-attachments/assets/67cfd090-5335-4680-abc1-4744c3721a0d)


## Future Enhancements
- Add more job sources and unify the data pipeline.
- Implement data quality checks in the Airflow DAGs.
- Visualize the job trends using tools like Grafana or Metabase.
Author: Rokas Klydzia

Feel free to contribute or raise issues for any problems or suggestions you have!

```arduino
This README provides a comprehensive overview of your project and should be helpful for setting it up, running it, and troubleshooting any issues.
```
