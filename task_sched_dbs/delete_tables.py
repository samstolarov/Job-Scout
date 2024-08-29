#Only for testing purposes

import time
import boto3

def delete_tables():
    # Initialize DynamoDB client
    dynamodb = boto3.client('dynamodb', region_name='us-east-2')

    # List of table names to delete
    table_names = ['tasks', 'executions', 'history']

    # Delete tables
    for table_name in table_names:
        try:
            dynamodb.delete_table(TableName=table_name)
            print(f"\033[91mTable {table_name} deleted successfully.\033[0m")
        except dynamodb.exceptions.ResourceNotFoundException:
            print(f"Table {table_name} not found.")
        except Exception as e:
            print(f"Error deleting table {table_name}: {e}")

