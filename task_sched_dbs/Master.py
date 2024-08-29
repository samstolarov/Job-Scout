import time
import threading
import random
import json
from datetime import datetime,timezone
import boto3
from task_sched_dbs.Tables import Refresh, Task
from task_sched_dbs.Scheduler import Scheduler
from task_sched_dbs.SQS_Impl import Impl
import isodate
from decimal import Decimal

class Executer:
    def __init__(self, dynamodb, segment_start: int, segment_end: int, exec_id: int, sqs_impl: Impl):
        self.dynamodb = dynamodb
        self.segment_start = segment_start
        self.segment_end = segment_end
        self.executions_table = self.dynamodb.Table('executions')
        self.history_table = self.dynamodb.Table('history')
        self.tasks_table = self.dynamodb.Table('tasks')
        self.exec_id = exec_id
        self.sqs_impl = sqs_impl

    def get_tasks(self, current_time):
        tasks = []
        for segment in range(self.segment_start, self.segment_end + 1):
            try:
                response = self.executions_table.query(
                    IndexName='next_exec_time-task_id-index',
                    KeyConditionExpression=boto3.dynamodb.conditions.Key('next_exec_time').eq(current_time),
                    FilterExpression=boto3.dynamodb.conditions.Attr('segment').eq(segment)
                )
                for item in response['Items']:
                    task_id = int(item['task_id'])
                    seg = int(item['segment'])
                    tasks.append((task_id, seg))
            except Exception as e:
                print(f"Error querying tasks for segment {segment}: {e}")
        return tasks

    def process_tasks(self, current_time):
        tasks = self.get_tasks(current_time)
        for task_id, segment in tasks:
            print("\033[95mexecuter: " + str(self.exec_id) + " | time: " + str(current_time) + "\033[0m")
            #self.publish_to_kafka(task_id)
            self.add_to_history_data(task_id, current_time, "success", 1)
            self.update_next_execution(task_id, current_time, segment)
            
            task_type = self.get_task_type(task_id)
            self.send_sqs_message(task_id, task_type)
            
    def update_next_execution(self, task_id, current_time, segment):
        # Retrieve the interval for the task from the tasks table
        response = self.tasks_table.get_item(
            Key={'task_id': task_id}
        )
        task = response['Item']
        interval = task['interval']
        created = task['created']

        # Parse the interval and add it to the current_time to get the new next_exec_time
        new_next_exec_time = self.calculate_next_exec_time(current_time, interval)

        # Update the next_exec_time in the executions table
        try:
            # Query using GSI to retrieve items based on task_id
            response = self.executions_table.get_item(
                Key={
                    'segment': segment,
                    'task_id': task_id
                }
            )
            if 'Item' in response:
                item = response['Item']
                # Update the item using UpdateItem operation
                response = self.executions_table.update_item(
                    Key={
                        'segment': segment,
                        'task_id': task_id
                    },
                    UpdateExpression="SET next_exec_time = :val",
                    ExpressionAttributeValues={
                        ':val': new_next_exec_time
                    },
                    ReturnValues="UPDATED_NEW"
                )
                print(f"\033[93mUpdate for task_id={task_id} in executions table complete.\033[0m")
            else:
                print(f"Item for task_id={task_id} and segment={segment} not found.")

        except Exception as e:
            print(f"Error updating next_exec_time for task_id={task_id}: {e}")
        
    def calculate_next_exec_time(self, current_time, interval):
        # Parse the interval (assuming it's in ISO 8601 format like 'PT1M')
        duration = isodate.parse_duration(interval)
        
        # Calculate the new next_exec_time as Unix timestamp
        new_next_exec_time = current_time + duration.total_seconds()
        return int(new_next_exec_time)
    
    def add_to_history_data(self, task_id, exec_time, status, retries):
        # Convert task_id to int if necessary
        task_id = int(task_id)
        #print("woohoo")
        
        # Insert item into historydata table
        self.history_table.put_item(
            Item={
                'task_id': task_id,
                'exec_time': exec_time,
                'status': status,
                'retries': retries
            }
        )
        print(f"\033[93mAdded task_id={task_id} to historydata table with status '{status}' and {retries} retries.\033[0m")

    def publish_to_kafka(self, task):
        # Placeholder logic for publishing task to Kafka
        print(f"\033[93mPublishing task {task} to Kafka\033[0m")
    
    def send_sqs_message(self, task_id, task_type):
        message_body = {
            'task_id': task_id,
            'task_type': task_type,
            'exec_id': self.exec_id,
            'timestamp': int(datetime.now().timestamp())
        }
        
        if task_type == 'notif':
            task_details = self.get_notif_details(task_id)
            message_body.update(task_details)
        elif task_type == 'refresh':
            print("this is a refresh")
        else:
            print("uh oh- bad type")
        
        self.sqs_impl.send_message(task_type, json.dumps(message_body, cls=DecimalEncoder))
        messages = self.sqs_impl.receive_messages(task_type)
        for message in messages:
                print(f"Received message: {message['Body']}")
                self.sqs_impl.delete_message(message['ReceiptHandle'], task_type)

    def get_notif_details(self, task_id):
        response = self.tasks_table.get_item(
            Key={'task_id': task_id}
        )
        task = response['Item']
        user_id = str(task.get('user_id'))
        
        # Optional fields
        job_id = task.get('job_id')
        title = task.get('title')
        company = task.get('company')
        location = task.get('location')
        
        notif_details = {
            'user_id': user_id,
            'job_id': job_id,
            'title': title,
            'company': company,
            'location': location
        }
        
        # Remove keys with None values
        notif_details = {k: v for k, v in notif_details.items() if v is not None}
        
        return notif_details
    
    def get_task_type(self, task_id):
        # Example logic to determine task type based on task_id
        # You need to implement your own logic based on your task_id patterns
        response = self.tasks_table.get_item(
            Key={'task_id': task_id}
        )
        task = response['Item']
        return task['type']



class Master:
    def __init__(self, schedule_instances:int = 1):
        self.scheduler = Scheduler(schedule_instances+1)
        self.dyna = self.scheduler.table_set.dynamodb
        self.exec_number = 0
        self.executer_count = (schedule_instances + 3) // 4  # This ensures ceil(schedule_instances / 4)
        if self.executer_count < 1:
            self.executer_count = 1

        self.sqs_impl = Impl()

        self.executers = []
        
        # Create Executer instances with assigned ranges
        for i in range(self.executer_count):
            # Calculate segment ranges for each Executer
            min_seg = i * 4 + 1
            max_seg = min(min_seg + 3, schedule_instances)
            
            # Create Executer instance and add to list
            self.executers.append(Executer(self.dyna, min_seg, max_seg, self.get_next_exec_number(), self.sqs_impl))
        
    def add_task(self, task: Task) -> int:
        self.scheduler.add_task(task)
        return task.task_id

    def delete_task(self, task_id: int):
        try:
            self.scheduler.delete_task(task_id)
            return {"status": "success", "message": f"Task {task_id} deleted successfully"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    # def update_task(self, task_id: int, new_task: Task):
    #     try:
    #         self.scheduler.update_task(task_id, new_task)
    #         return {"status": "success", "message": f"Task {task_id} updated successfully"}
    #     except Exception as e:
    #         return {"status": "error", "message": str(e)}
        
    def get_task(self, task_id: int) -> Task:
        return self.scheduler.get_task(task_id)

    def run(self):
        while True:
            current_time = self.get_current_time()
            self.update_executers(current_time)
            #print("current time: " + str(current_time))
            time.sleep(60)  # Sleep for 60 seconds

    def update_executers(self, current_time):
        for executer in self.executers:
            executer.process_tasks(current_time)

    def get_current_time(self) -> int:
        # Get current time as an integer timestamp rounded to the nearest minute
        now = datetime.now(timezone.utc)
        return self.get_unix_timestamp_by_min(now)

    def get_unix_timestamp_by_min(self, dt: datetime) -> int:
        # Set seconds and microseconds to zero
        dt = dt.replace(second=0, microsecond=0)
        # Convert to UNIX timestamp
        return int(dt.timestamp())
    
    def get_next_exec_number(self) -> int:
        self.exec_number += 1
        return self.exec_number
    
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def run_master_in_background(master):
    master.run()

if __name__ == "__main__":
    print("\033[92mStarting Master...\033[0m")
    master = Master(18)
    
    # Create a thread for running master.run() in the background
    master_thread = threading.Thread(target=run_master_in_background, args=(master,))
    master_thread.daemon = True  # Set daemon to True so it exits when the main thread exits
    master_thread.start()

    # Now continue with your main scheduler logic, receiving tasks and updating databases
    print("\033[92mRunning Scheduler and Receiving Tasks...\033[0m")
    while True:
        rando = random.randint(1,10000)
        print("\033[96mnow inserting: " + str(rando) + "\033[0m")
        # Your scheduler logic to receive new tasks and update databases
        new_task = Refresh(
            task_id=rando,
            interval="PT1M",
            retries=3,
            created=int(datetime.now().timestamp()),
            last_refresh=0,
            type = "refresh"
        )
        master.add_task(new_task)
        time.sleep(10)
        
