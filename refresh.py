import boto3
import json
import logging
import time
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from bs4 import BeautifulSoup
import requests
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS configuration
AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
REFRESH_QUEUE_URL = os.getenv("REFRESH_QUEUE_URL")

logger.info(f"Loaded AWS configuration: REGION={AWS_REGION}, QUEUE_URL={REFRESH_QUEUE_URL}")

# Initialize SQS client
sqs_client = boto3.client(
    'sqs',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Initialize DynamoDB resource
dynamodb = boto3.resource(
    'dynamodb',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)
table = dynamodb.Table('Jobs')

job_counter = 0
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Scraper function
def linkedin_scraper(job_title, location, page_number):
    global job_counter
    base_url = 'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={job_title}&location={location}&trk=public_jobs_jobs-search-bar_search-submit&position=1&pageNum=0&start='
    formatted_url = base_url.format(job_title=job_title.replace(' ', '%20'), location=location.replace(' ', '%20'))
    next_page = formatted_url + str(page_number)
    logger.info(f"Scraping URL: {next_page}")

    response = requests.get(next_page, headers=headers, verify=False)

    if response.status_code != 200:
        logger.error(f"Failed to retrieve page {page_number}. Status code: {response.status_code}")
        return

    soup = BeautifulSoup(response.content, 'html.parser')

    jobs = soup.find_all('div', class_='base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card')

    if not jobs:
        logger.info(f"No jobs found on page {page_number}. Ending scrape.")
        return

    for job in jobs:
        job_title = job.find('h3', class_='base-search-card__title').text.strip()
        job_company = job.find('h4', class_='base-search-card__subtitle').text.strip()
        job_location = job.find('span', class_='job-search-card__location').text.strip()
        job_link = job.find('a', class_='base-card__full-link')['href']

        job_counter += 1
        
        job_id = job_counter  # job_id incremented for every job added

        job_data = {
            'job_id': int(job_id),
            'title': job_title,
            'company': job_company,
            'location': job_location,
            'link': job_link,
            'description': 'N/A'  # Description is not scraped, set a default value
        }

        # Save job data to DynamoDB
        try:
            table.put_item(Item=job_data)
            logger.info(f"Inserted job {job_id} into DynamoDB")
        except (NoCredentialsError, PartialCredentialsError) as e:
            logger.error(f"Failed to insert job {job_id} into DynamoDB: {str(e)}")

    logger.info(f"Data updated with {len(jobs)} jobs from page {page_number}")

    if len(jobs) == 0 or page_number >= 100:
        return

    # Adding a delay to avoid hitting the server too quickly
    time.sleep(1)

    linkedin_scraper(job_title, location, page_number + 25)

# Function to process messages from Refresh Queue
def process_refresh_message():
    try:
        response = sqs_client.receive_message(
            QueueUrl=REFRESH_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )
        
        messages = response.get('Messages', [])
        logger.info(f'Response from SQS: {response}')
        
        if not messages:
            logger.info('No messages in the refresh queue.')
            return
        
        for message in messages:
            logger.info(f'Received refresh message: {message}')
            try:
                # Call the Linkedin scraper (refreshing the database)
                #linkedin_scraper("default_job_title", "default_location", 0)
                jobspy_scraper = JobScraper()
                jobspy_scraper.scrape_jobs()
                jobspy_scraper.add_jobs_to_db_from_json("jobs.json")
                # Delete the message from the queue after processing
                sqs_client.delete_message(
                    QueueUrl=REFRESH_QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )
                logger.info(f'Message deleted from refresh queue: {message["MessageId"]}')
            except KeyError as e:
                logger.error(f'Error processing refresh message: Missing key {e}')
            except json.JSONDecodeError as e:
                logger.error(f'Error processing refresh message: JSON decode error {e}')
    except (NoCredentialsError, PartialCredentialsError) as e:
        logger.error(f'Credentials error: {str(e)}')
    except Exception as e:
        logger.error(f'Unexpected error: {str(e)}')

if __name__ == "__main__":
    while True:
        process_refresh_message()