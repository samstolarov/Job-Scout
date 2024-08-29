import requests
from bs4 import BeautifulSoup
import json
import time
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


class Scraper:
    def __init__(self, region_name='us-east-2', table_name='Jobs'):
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
        self.table = self.dynamodb.Table(table_name)
        self.data = []

    def linkedin_scraper(self, page_number=0, job_counter:int = 81):
        base_url = 'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?trk=public_jobs_jobs-search-bar_search-submit&position=1&pageNum=0&start='
        next_page = base_url + str(page_number)
        print(f"Scraping URL: {next_page}")

        response = requests.get(next_page, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }, verify=False)

        if response.status_code != 200:
            print(f"Failed to retrieve page {page_number}. Status code: {response.status_code}")
            return

        soup = BeautifulSoup(response.content, 'html.parser')

        jobs = soup.find_all('div', class_='base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card')

        if not jobs:
            print(f"No jobs found on page {page_number}. Ending scrape.")
            return

        for job in jobs:
            job_title = job.find('h3', class_='base-search-card__title').text.strip()
            job_company = job.find('h4', class_='base-search-card__subtitle').text.strip()
            job_location = job.find('span', class_='job-search-card__location').text.strip()
            job_link = job.find('a', class_='base-card__full-link')['href']

            job_counter += 1

            job_data = {
                'job_id': int(job_counter),
                'title': job_title,
                'company': job_company,
                'location': job_location,
                'link': job_link,
                'description': 'N/A'  # Description is not scraped, set a default value
            }

            # Save job data to DynamoDB
            try:
                self.table.put_item(Item=job_data)
                print(f"Inserted job {job_counter} into DynamoDB")
            except (NoCredentialsError, PartialCredentialsError) as e:
                print(f"Failed to insert job {job_counter} into DynamoDB: {str(e)}")

        print(f"Data updated with {len(jobs)} jobs from page {page_number}")

        if len(jobs) == 0 or page_number >= 100:
            with open('linkedin-jobs.json', 'w', encoding='utf-8') as json_file:
                json.dump(self.data, json_file, ensure_ascii=False, indent=4)
            print('File closed')
            return

        # Adding a delay to avoid hitting the server too quickly
        time.sleep(1)

        self.linkedin_scraper(page_number + 25, job_counter)


if __name__ == "__main__":
    scraper = Scraper()
    scraper.linkedin_scraper()
