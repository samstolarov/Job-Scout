import json
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from jobspy import scrape_jobs
from datetime import date, datetime
from decimal import Decimal
import math


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def convert_to_decimals(item):
    for key, value in item.items():
        if isinstance(value, float):
            if math.isfinite(value):
                item[key] = Decimal(str(value))
            else:
                item[key] = None  # or some default value
        elif isinstance(value, dict):
            convert_to_decimals(value)
    return item


class JobScraper:
    def __init__(self, region_name='us-east-2', table_name='Jobs'):
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
        self.table = self.dynamodb.Table(table_name)
        self.jobs = None

    def scrape_jobs(self):
        # Hardcoded parameters
        site_names = ["indeed", "linkedin", "zip_recruiter", "glassdoor"]
        search_term = ""  # Broad search
        location = ""     # Broad search
        results_wanted = 20
        hours_old = 72
        country_indeed = 'USA'
        proxies = None  # No proxies by default

        self.jobs = scrape_jobs(
            site_name=site_names,
            search_term=search_term,
            location=location,
            results_wanted=results_wanted,
            hours_old=hours_old,
            country_indeed=country_indeed,
            proxies=proxies
        )
        self.save_jobs_to_json("jobs.json")
        return self.jobs

    def save_jobs_to_json(self, file_path):
        if self.jobs is not None:
            jobs_json = self.jobs.to_dict(orient='records')
            with open(file_path, "w") as file:
                json.dump(jobs_json, file, indent=4, cls=DateTimeEncoder)

    def add_jobs_to_db_from_json(self, file_path):
        try:
            with open(file_path, "r") as file:
                jobs = json.load(file, parse_float=Decimal)
            job_counter = 1
            for job in jobs:
                if isinstance(job, dict):  # Ensure job is a dictionary
                    item = {
                        'job_id': job_counter,  # Generate unique job_id as number
                        # Use 'title' from JSON
                        'title': job.get('title', 'N/A'),
                        'company': job.get('company', 'N/A'),
                        'location': job.get('location', 'N/A'),
                        'link': job.get('job_url', 'N/A'),
                        'description': job.get('description', 'N/A')
                    }
                    item = convert_to_decimals(item)
                    try:
                        self.table.put_item(Item=item)
                        print(f"Added job {item['job_id']} to DynamoDB")
                        job_counter += 1  # Increment the job counter
                    except (NoCredentialsError, PartialCredentialsError) as e:
                        print(f"Credentials error: {str(e)}")
                    except Exception as e:
                        print(
                            f"Error adding job {item['job_id']} to DynamoDB: {str(e)}")
        except Exception as e:
            print(f"Error reading JSON file: {str(e)}")


if __name__ == "__main__":
    scraper = JobScraper()
    scraper.scrape_jobs()
    scraper.add_jobs_to_db_from_json("jobs.json")
