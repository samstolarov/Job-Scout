import boto3
import json
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, NoRegionError
from boto3.dynamodb.conditions import Attr

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS configuration
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")  # Provide default region if not set
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
NOTIFS_QUEUE_URL = os.getenv("NOTIFS_QUEUE_URL")

# Email configuration
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = os.getenv("SMTP_PORT", 587)  # Provide default SMTP port if not set
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
MAIL_FROM = os.getenv("MAIL_FROM")

# Check if essential environment variables are missing
if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, NOTIFS_QUEUE_URL, SMTP_SERVER, SMTP_USERNAME, SMTP_PASSWORD, MAIL_FROM]):
    logger.error("One or more essential environment variables are missing.")
    raise SystemExit("Missing environment variables")

# Initialize SQS client
try:
    sqs_client = boto3.client(
        'sqs',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
except NoRegionError as e:
    logger.error(f"AWS region not specified: {str(e)}")
    raise

# Initialize DynamoDB resource
try:
    dynamodb = boto3.resource(
        'dynamodb',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
except NoRegionError as e:
    logger.error(f"AWS region not specified: {str(e)}")
    raise

# Function to send email
def send_email(subject: str, recipients: list, body: str):
    try:
        server = smtplib.SMTP(host=SMTP_SERVER, port=SMTP_PORT)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)

        msg = MIMEMultipart()
        msg['From'] = MAIL_FROM
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        server.sendmail(MAIL_FROM, recipients, msg.as_string())
        server.quit()
    except Exception as e:
        logger.error(f"Error sending email: {str(e)}")

# Function to process messages from Notifs Queue
def process_notifs_message():
    try:
        response = sqs_client.receive_message(
            QueueUrl=NOTIFS_QUEUE_URL,
            MaxNumberOfMessages=5,
            WaitTimeSeconds=10
        )
        if 'Messages' in response:
            for message in response['Messages']:
                body = json.loads(message['Body'])
                job_title = body.get('title', '')
                location = body.get('location', '')
                company = body.get('company', '')
                user_id = str(body.get('user_id', ''))  # Ensure user_id is a string

                logger.info(f"LOGGING, USER ID: {user_id}")

                email = get_user_email(user_id)

                # Perform search
                search_results = perform_search(job_title, location, company)
                print(search_results)
                # Send email with results
                send_email(
                    subject=f"Job Search Results for {job_title} in {location} at {company}",
                    recipients=[email],
                    body=f"Here are the job search results for {job_title} in {location} at {company}:\n\n{search_results}"
                )

                sqs_client.delete_message(
                    QueueUrl=NOTIFS_QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )
    except Exception as e:
        logger.error(f"Error processing notifs message: {str(e)}")

# Function to perform search
def perform_search(job_title: str, location: str, company: str):
    table = dynamodb.Table('Jobs')
    search_results = []

    # Scan the table for matching job_title and location
    response = table.scan(
        FilterExpression=Attr('title').contains(job_title) & Attr('location').contains(location) & Attr('company').contains(company)
    )
    
    for job in response['Items']:
        search_results.append({
            'title': str(job.get('title', 'N/A')),
            'company': str(job.get('company', 'N/A')),
            'location': str(job.get('location', 'N/A')),
            'link': str(job.get('link', '#'))
        })

    return search_results if search_results else [{"title": "No matching jobs found.", "company": "", "location": "", "link": ""}]

def get_user_email(user_id: str) -> str:
    # Reference the Users table
    table = dynamodb.Table('Users')
    print("LOGGING, USER ID: " + user_id)
    
    try:
        # Perform the query
        response = table.get_item(
            Key={'id': user_id}
        )
        
        # Check if the user was found
        if 'Item' in response:
            # Extract and return the email address
            return response['Item'].get('email', 'Email not found')
        else:
            return 'User not found'
    except Exception as e:
        print(f"Error querying table: {str(e)}")
        return 'Error querying table'
    
if __name__ == "__main__":
    while True:
        process_notifs_message()
