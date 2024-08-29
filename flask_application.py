import os
from flask import Flask, redirect, url_for, session, jsonify, request
from authlib.integrations.flask_client import OAuth
import boto3
from botocore.exceptions import ClientError

app = Flask(__name__)
app.secret_key = "CS_class_of_2027"

app.config['GOOGLE_ID'] = os.getenv('GOOGLE_ID')
app.config['GOOGLE_SECRET'] = os.getenv('GOOGLE_SECRET')
app.config['GOOGLE_REDIRECT_URI'] = os.getenv('GOOGLE_REDIRECT_URI')

# Initialize DynamoDB client and resource
AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

dynamodb_client = boto3.client(
    'dynamodb',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

dynamodb_resource = boto3.resource(
    'dynamodb',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Function to create DynamoDB table
def create_users_table():
    try:
        table = dynamodb_client.create_table(
            TableName='Users',
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        print("Table created successfully!")
        return table
    except ClientError as e:
        print(f"Error creating table: {e}")
        return None

# Check if table exists and create if it doesn't
try:
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if 'Users' not in existing_tables:
        create_users_table()
    else:
        print("Table 'Users' already exists.")
    users_table = dynamodb_resource.Table('Users')
except ClientError as e:
    print(f"Error listing tables or initializing table resource: {e}")

oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id=app.config['GOOGLE_ID'],
    client_secret=app.config['GOOGLE_SECRET'],
    authorize_url='https://accounts.google.com/o/oauth2/auth',
    authorize_params=None,
    access_token_url='https://oauth2.googleapis.com/token',
    access_token_params=None,
    refresh_token_url=None,
    client_kwargs={
        'scope': 'openid email profile',
        'prompt': 'select_account'
    },
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration'
)

@app.route('/')
def index():
    return 'You are currently not logged in.'

@app.route('/login')
def login():
    redirect_uri = url_for('authorize', _external=True)
    return google.authorize_redirect(redirect_uri)

@app.route('/logout')
def logout():
    session.pop('user', None)
    return redirect('http://ec2-18-191-83-191.us-east-2.compute.amazonaws.com:8501')

@app.route('/callback')
def authorize():
    token = google.authorize_access_token()
    resp = google.get('https://openidconnect.googleapis.com/v1/userinfo')
    resp.raise_for_status()
    user_info = resp.json()

    print(f"User info retrieved: {user_info}")  # Print user info to verify

    # Check if user already exists in DynamoDB
    try:
        response = users_table.get_item(Key={'id': user_info['sub']})
        if 'Item' in response:
            user_item = response['Item']
            print("User found in DynamoDB:", user_item)
            # Update session with existing user information
            session['user'] = user_item
        else:
            # Store new user information in DynamoDB
            print("Storing new user in DynamoDB")
            print(user_info['sub'])
            user_item = {
                'id': user_info['sub'],  # Assuming 'sub' is the unique identifier
                'email': user_info['email'],
                'name': user_info.get('name', ''),
            }
            users_table.put_item(Item=user_item)
            session['user'] = user_info  # Update session with new user information
    except ClientError as e:
        print(f"Error retrieving or storing user in DynamoDB: {e}")
    print("you made it here")
    return redirect(f'http://ec2-18-191-83-191.us-east-2.compute.amazonaws.com:8502?user_id={user_info["sub"]}')  # Redirect to Streamlit app with user ID



if __name__ == '__main__':
    app.run(port=8080)
