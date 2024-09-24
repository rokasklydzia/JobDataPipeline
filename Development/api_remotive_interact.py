import requests

def fetch_jobs():
    url = "https://remotive.io/api/remote-jobs"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['jobs']
    else:
        print(f"Failed to fetch jobs, status code: {response.status_code}")
        return None

jobs = fetch_jobs()
if jobs:
    print(f"Fetched {len(jobs)} jobs")

print(jobs)
