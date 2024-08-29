import boto3

class Impl:
    def __init__(self):
        print("\033[92mAWS initializing...\033[0m")
        self.sqs_client = boto3.client('sqs', region_name='us-east-2')

        # Create SQS queues
        self.refresh_queue_url = self.sqs_client.create_queue(QueueName='refresh-queue')['QueueUrl']
        self.notif_queue_url = self.sqs_client.create_queue(QueueName='notif-queue')['QueueUrl']

        # Get the ARN of the SQS queues
        self.refresh_queue_arn = self.sqs_client.get_queue_attributes(
            QueueUrl=self.refresh_queue_url,
            AttributeNames=['QueueArn']
        )['Attributes']['QueueArn']
        self.notif_queue_arn = self.sqs_client.get_queue_attributes(
            QueueUrl=self.notif_queue_url,
            AttributeNames=['QueueArn']
        )['Attributes']['QueueArn']

        print(f"Created SQS queue for refresh: {self.refresh_queue_url} with ARN: {self.refresh_queue_arn}")
        print(f"Created SQS queue for notif: {self.notif_queue_url} with ARN: {self.notif_queue_arn}")

    def send_message(self, type: str, message_body):
        if type == 'refresh':
            queue_url = self.refresh_queue_url
            queue_type = 'refresh'
        elif type == 'notif':
            queue_url = self.notif_queue_url
            queue_type = 'notification'
        else:
            print(f"Invalid type: {type}")
            return

        try:
            response = self.sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body
            )
            print(f"Message sent to {queue_type} queue: {response.get('MessageId')}")
        except Exception as e:
            print(f"Error sending message to {queue_type} queue: {e}")
    
    def receive_messages(self, type: str, max_messages=1, wait_time_seconds=10):
        queue_url = self.refresh_queue_url if type == 'refresh' else self.notif_queue_url if type == 'notif' else None
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time_seconds
            )
            messages = response.get('Messages', [])
            return messages
        except Exception as e:
            print(f"Error receiving messages: {e}")
            raise
    
    def delete_message(self, receipt_handle, type):
        queue_url = self.refresh_queue_url if type == 'refresh' else self.notif_queue_url if type == 'notif' else None
        try:
            self.sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            print(f"Message deleted from {type} queue")
        except Exception as e:
            print(f"Error deleting message from {type} queue: {e}")