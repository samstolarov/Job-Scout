import boto3

def delete_aws_resources():
    sqs_client = boto3.client('sqs', region_name='us-east-2')

    # List of queue names to delete
    sqs_queues = ['refresh-queue', 'notif-queue']

    # Delete SQS queues
    for queue_name in sqs_queues:
        try:
            queue_url = sqs_client.get_queue_url(QueueName=queue_name)['QueueUrl']
            sqs_client.delete_queue(QueueUrl=queue_url)
            print(f"\033[91mDeleted SQS queue: {queue_url}\033[0m")
        except Exception as e:
            print(f"Error deleting SQS queue {queue_name}: {e}")

if __name__ == "__main__":
    delete_aws_resources()
