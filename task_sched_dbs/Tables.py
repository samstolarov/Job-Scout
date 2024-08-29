import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from pydantic import BaseModel, Field

def get_current_time() -> int:
    now = datetime.now(timezone.utc)
    return get_unix_timestamp_by_min(now)

def get_unix_timestamp_by_min(dt: datetime) -> int:
    dt = dt.replace(second=0, microsecond=0)
    return int(dt.timestamp())

class Task(BaseModel):
    task_id: int
    interval: str
    retries: int
    created: int = Field(default_factory=get_current_time)
    type: str

class Refresh(Task):
    last_refresh: int

class Notifs(Task):
    user_id: int
    job_id: int = None
    title: str = None
    company: str = None
    location: str = None

class HistoryData(BaseModel):
    task_id: int
    exec_time: int
    status: str
    retries: int

class ExecutionsData(BaseModel):
    next_exec_time: int
    task_id: int
    segment: int

class Tables:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-2')

    def create_table(self, table_name, key_schema, attribute_definitions, provisioned_throughput, global_secondary_indexes=None):
        try:
            table_params = {
                'TableName': table_name,
                'KeySchema': key_schema,
                'AttributeDefinitions': attribute_definitions,
                'ProvisionedThroughput': provisioned_throughput
            }
            if global_secondary_indexes:
                table_params['GlobalSecondaryIndexes'] = global_secondary_indexes

            # Create the table
            table = self.dynamodb.create_table(**table_params)

            # Wait until the table exists
            table.wait_until_exists()

            # Print the table status
            print(f"\033[92mTable {table_name} created successfully. Status: {table.table_status}\033[0m")
        except self.dynamodb.meta.client.exceptions.ResourceInUseException:
            print(f"Table '{table_name}' already exists.")
        except ClientError as e:
            print(f"Error creating table: {str(e)}")


    def create_jobs_table(self, table_name='Jobs'):
        try:
            response = self.dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'job_id', 'KeyType': 'HASH'}  # Partition key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'job_id', 'AttributeType': 'N'}  # Number
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            # Wait until the table exists
            response.wait_until_exists()
            print(f"Job Table creation initiated. Status: {response.table_status}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceInUseException':
                print(f"Table '{table_name}' already exists.")
            else:
                print(f"Error creating table: {str(e)}")
    
    def create_users_table(self):
        try:
            # Define the table schema
            table = self.dynamodb.create_table(
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
                        'AttributeType': 'S'  # String
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            
            # Wait until the table exists
            table.wait_until_exists()

            # Print the table status
            print(f"\033[92mTable 'Users' created successfully. Status: {table.table_status}\033[0m")
        except self.dynamodb.meta.client.exceptions.ResourceInUseException:
            print("Table 'Users' already exists.")
        except ClientError as e:
            print(f"Error creating table: {str(e)}")

    def initialize_tables(self):
        tables = {
            'tasks': {
                'key_schema': [
                    {'AttributeName': 'task_id', 'KeyType': 'HASH'}
                ],
                'attribute_definitions': [
                    {'AttributeName': 'task_id', 'AttributeType': 'N'},
                    {'AttributeName': 'user_id', 'AttributeType': 'N'},
                ],
                'provisioned_throughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5},
                'global_secondary_indexes': [
                    {
                        'IndexName': 'user_id-index',
                        'KeySchema': [
                            {'AttributeName': 'user_id', 'KeyType': 'HASH'}
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL'
                        },
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    }
                ]
            },
            'executions': {
                'key_schema': [
                    {'AttributeName': 'task_id', 'KeyType': 'HASH'},
                    {'AttributeName': 'segment', 'KeyType': 'RANGE'}
                ],
                'attribute_definitions': [
                    {'AttributeName': 'segment', 'AttributeType': 'N'},
                    {'AttributeName': 'next_exec_time', 'AttributeType': 'N'},
                    {'AttributeName': 'task_id', 'AttributeType': 'N'}
                ],
                'provisioned_throughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5},
                'global_secondary_indexes': [
                    {
                        'IndexName': 'next_exec_time-task_id-index',
                        'KeySchema': [
                            {'AttributeName': 'next_exec_time', 'KeyType': 'HASH'}
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL'
                        },
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    }
                ]
            },
            'history': {
                'key_schema': [
                    {'AttributeName': 'task_id', 'KeyType': 'HASH'},
                    {'AttributeName': 'exec_time', 'KeyType': 'RANGE'}
                ],
                'attribute_definitions': [
                    {'AttributeName': 'task_id', 'AttributeType': 'N'},
                    {'AttributeName': 'exec_time', 'AttributeType': 'N'},
                ],
                'provisioned_throughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            }
        }
        for table_name, table_config in tables.items():
            self.create_table(table_name,
                              table_config['key_schema'],
                              table_config['attribute_definitions'],
                              table_config['provisioned_throughput'],
                              table_config.get('global_secondary_indexes'))
        self.create_jobs_table()
        self.create_users_table()

if __name__ == "__main__":
    print("Initializing tables...")
    table = Tables()
    table.initialize_tables()
