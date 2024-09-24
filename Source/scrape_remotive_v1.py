import requests
from bs4 import BeautifulSoup
import pandas as pd

# URL of the Remotive job board page
url = "https://remotive.com/remote-jobs"

# Send an HTTP request to the URL
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")
    
    # List to store job details
    jobs = []

    # Find all job listings on the page
    job_listings = soup.find_all("div", class_="job-listing-item")

    # Loop through each job listing and extract details
    for job in job_listings:
        # Extract job title
        title_element = job.find("h2", class_="job-title")
        title = title_element.text.strip() if title_element else "Not specified"

        # Extract company name
        company_element = job.find("div", class_="company")
        company = company_element.text.strip() if company_element else "Not specified"

        # Extract job location
        location_element = job.find("span", class_="location")
        location = location_element.text.strip() if location_element else "Remote"

        # Extract job type (e.g., full-time, part-time)
        job_type_element = job.find("div", class_="category")
        job_type = job_type_element.text.strip() if job_type_element else "Not specified"

        # Extract the link to the job ad
        link_element = job.find("a", class_="job-listing-item__link")
        link = link_element['href'] if link_element else "Not specified"

        # Append the job details to the jobs list
        jobs.append({
            'title': title,
            'company': company,
            'location': location,
            'job_type': job_type,
            'link': f"https://remotive.com{link}"
        })

    # Convert the jobs list to a DataFrame
    jobs_df = pd.DataFrame(jobs)

    # Save the DataFrame to a CSV file
    jobs_df.to_csv("remotive_jobs.csv", index=False)

    print(f"Scraped {len(jobs)} job listings and saved to remotive_jobs.csv")
else:
    print(f"Failed to retrieve the page. Status code: {response.status_code}")
