import boto3
import time

class HelloSQS:
    def __init__(self, queue_name):
        self.sqs = boto3.resource('sqs')
        self.queue = self.create_queue(queue_name)
    
    def create_queue(self, queue_name):
        try:
            queue = self.sqs.create_queue(QueueName=queue_name)
            print(f"Created SQS queue with URL: {queue.url}")
            return queue
        except Exception as e:
            print(f"Error creating SQS queue: {e}")
            raise
    
    def send_message(self, message_body):
        try:
            response = self.queue.send_message(MessageBody=message_body)
            print(f"Sent message: {message_body}")
            return response
        except Exception as e:
            print(f"Error sending message: {e}")
            raise
    
    def receive_messages(self, max_messages=1, wait_time_seconds=5):
        try:
            messages = self.queue.receive_messages(MaxNumberOfMessages=max_messages, WaitTimeSeconds=wait_time_seconds)
            for message in messages:
                print(f"Received message: {message.body}")
                message.delete()  # Delete the message from the queue
        except Exception as e:
            print(f"Error receiving messages: {e}")
            raise

# Example usage:
if __name__ == "__main__":
    sqs_client = HelloSQS('refresh-queue')
    
    # Send a message
    message_body = "Hello, SQS!"
    sqs_client.send_message(message_body)
    
    # Wait for a moment before receiving messages
    time.sleep(1)
    
    # Receive messages
    sqs_client.receive_messages() 
