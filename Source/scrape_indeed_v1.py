import asyncio
from pyppeteer import launch
import csv


async def scrape_indeed():
    browser = await launch(headless=False, executablePath='C:/chrome-win/chrome.exe')
    page = await browser.newPage()

    # Go to the Indeed website
    await page.goto('https://www.indeed.com')

    # Wait for the search fields to load
    await page.waitForSelector('#text-input-what')
    await page.waitForSelector('#text-input-where')

    # Enter the search criteria
    await page.type('#text-input-what', 'Data Engineer')
    await page.type('#text-input-where', 'USA')

    # Submit the search form
    await page.click('button[type="submit"]')

    # Wait for the results page to load
    await page.waitForNavigation()

    # Get the list of job postings
    job_listings = await page.querySelectorAll('.resultContent')

    # List to hold the scraped job data
    jobs = []

    for job in job_listings:
        # Extract the job title
        title_element = await job.querySelector('h2.jobTitle span[title]')
        title = await page.evaluate('(element) => element.textContent', title_element) if title_element else 'Not specified'

        # Extract the company name
        company_element = await job.querySelector('div.company_location [data-testid="company-name"]')
        company = await page.evaluate('(element) => element.textContent', company_element) if company_element else 'Not specified'

        # Extract the location
        location_element = await job.querySelector('div.company_location [data-testid="text-location"]')
        location = await page.evaluate('(element) => element.textContent', location_element) if location_element else 'Not specified'

        # Extract the link to the job ad
        link_element = await job.querySelector('h2.jobTitle a')
        link = await page.evaluate('(element) => element.href', link_element) if link_element else 'Not specified'

        # Extract the job type (e.g., full-time, part-time, contract)
        job_type_element = await job.querySelector('div.metadata [data-testid="job-type"]')
        job_type = await page.evaluate('(element) => element.textContent', job_type_element) if job_type_element else 'Not specified'

        # Extract the region (e.g., anywhere in the World, Europe)
        region_element = await job.querySelector('div.metadata [data-testid="location"]')
        region = await page.evaluate('(element) => element.textContent', region_element) if region_element else 'Not specified'

        # Extract the salary (if available)
        salary_element = await job.querySelector('div.metadata [data-testid="salary"]')
        salary = await page.evaluate('(element) => element.textContent', salary_element) if salary_element else 'Not specified'

        # Append the job data to the list
        jobs.append({
            'title': title,
            'company': company,
            'location': location,
            'link': link,
            'job_type': job_type,
            'region': region,
            'salary': salary
        })

    # Write to CSV file
    with open('jobs.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=jobs[0].keys())
        writer.writeheader()
        writer.writerows(jobs)

    # Close the browser
    await browser.close()


# Run the coroutine
if __name__ == '__main__':
    asyncio.run(scrape_indeed())
