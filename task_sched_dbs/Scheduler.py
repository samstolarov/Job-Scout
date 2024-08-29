from datetime import datetime
import isodate
from task_sched_dbs.Tables import Tables, Task, Refresh, Notifs
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key




class Scheduler:

    def __init__(self, max_seg: int = 2):
        self.table_set = Tables()
        self.segment_max = max_seg
        self.segment = 0
        self.initialize_tables()

    def initialize_tables(self):
        self.table_set.initialize_tables()

    def add_task(self, t: Task):
        try:
            task_item={
                'task_id': t.task_id,
                'interval': t.interval,
                'retries': t.retries,
                'created': self.get_unix_timestamp_by_min(datetime.fromtimestamp(t.created)),
                'type': t.type
            }
            # Handle specific task types
            if isinstance(t, Refresh):
                task_item['last_refresh'] = t.last_refresh
                
            elif isinstance(t, Notifs):
                task_item.update({
                    'user_id': t.user_id,
                    'job_id': t.job_id,
                    'title': t.title,
                    'company': t.company,
                    'location': t.location
                })
            else:
                raise UnknownTaskTypeError(t)
            self.table_set.dynamodb.Table('tasks').put_item(Item=task_item)
            print("\033[94mtask " + str(t.task_id) + " added to tasks table!\033[0m")
            self.table_set.dynamodb.Table('executions').put_item(
                Item={
                    'task_id': t.task_id,
                    'next_exec_time': self.get_unix_timestamp_by_min(datetime.now() + isodate.parse_duration(t.interval)),
                    'segment': self.get_next_segment()
                }
            )
            print("\033[94mtask " + str(t.task_id) + " added to EXECUTIONS table!\033[0m")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            print(f"Error putting item in DynamoDB: {error_code} - {error_message}")
            # Handle specific error cases here

        except Exception as e:
            print(f"Unexpected error: {e}")
            # Handle unexpected errors or log them for investigation
    
    def delete_task(self, task_id: int):
        try:
            segment = self.get_segment(task_id)
            if not segment:
                raise Exception(f"No segment found for task_id {task_id}")
            # Delete task from executions table
            self.table_set.dynamodb.Table('executions').delete_item(
                Key={'task_id': task_id, 'segment': segment}
            )
            # Delete task from tasks table
            self.table_set.dynamodb.Table('tasks').delete_item(
                Key={'task_id': task_id}
            )
        except Exception as e:
            raise Exception(f"Failed to delete task with task_id {task_id}: {e}")

    # def update_task(self, task_id: int, new_task: Task):
    #     # Determine the fields to update based on the task type
    #     update_expression = "set interval=:i"
    #     expression_attribute_values = {':i': new_task.interval}
        
    #     if new_task.type == 'notif':
    #         update_expression += ", user_id=:uid, email=:em, job_id=:jid, title=:ti, description=:de, company=:co, location=:lo"
    #         expression_attribute_values.update({
    #             ':uid': new_task.user_id,
    #             ':em': new_task.email,
    #             ':jid': new_task.job_id,
    #             ':ti': new_task.title,
    #             ':de': new_task.description,
    #             ':co': new_task.company,
    #             ':lo': new_task.location
    #         })

    #     # Update task in tasks table
    #     self.table_set.tasks.update_item(
    #         Key={'task_id': task_id},
    #         UpdateExpression=update_expression,
    #         ExpressionAttributeValues=expression_attribute_values
    #     )

    def get_segment(self, task_id: int):
        try:
            response = self.table_set.dynamodb.Table('executions').query(
                KeyConditionExpression=Key('task_id').eq(task_id)
            )
            items = response.get('Items', [])
            if items:
                return items[0].get('segment')
            else:
                raise Exception(f"No segment found for task_id {task_id}")
        except Exception as e:
            raise Exception(f"Failed to get segment for task_id {task_id}: {e}")
    
    def get_task(self, task_id: int):
        response = self.table_set.dynamodb.Table('tasks').query(
            KeyConditionExpression=Key('task_id').eq(task_id)
        )
        items = response.get('Items', [])
        if items:
            return items[0]
        else:
            raise Exception(f"No task found with task_id {task_id}")


    def get_next_segment(self):
        self.segment += 1
        if self.segment >= self.segment_max:
            self.segment = 1
        return self.segment
    
    def set_segment(self, segment:int):
        self.segment=segment

    def set_max_segment(self, seg_max:int):
        self.segment_max=seg_max
         
    def query_executions_by_next_exec_time(self, next_exec_time):
        try:
            response = self.table_set.dynamodb.Table('executions').query(
                KeyConditionExpression=Key('next_exec_time').eq(next_exec_time)
            )

            for item in response['Items']:
                print(item)  # Process each item as needed -- eventually return them so they can be executed

        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            print(f"Error querying executions table: {error_code} - {error_message}")
            # Handle specific error cases here

        except Exception as e:
            print(f"Unexpected error: {e}")
            # Handle unexpected errors or log them for investigation

    def get_unix_timestamp_by_min(self, dt: datetime) -> int:
       
        # Set seconds and microseconds to zero

        dt = dt.replace(second=0, microsecond=0)

        # Convert to UNIX timestamp
        unix_timestamp = int(dt.timestamp())
        return unix_timestamp
    
    def convert_datetime_to_iso8601(self, dt: datetime) -> str:
        # Convert datetime to ISO 8601 string
        return dt.isoformat()

class UnknownTaskTypeError(Exception):
        def __init__(self, task: Task):
            super().__init__(f"Unknown task type for task: {task.task_id}")


##TEST CODE##
if __name__ == "__main__":
    print("Creating Scheduler...")
    scheduler = Scheduler()
    new_task = Refresh(
        task_id=1,
        recurring=True,
        interval="PT1M",
        retries=3,
        created=int(datetime.now().timestamp()),
        last_refresh=0,
        type = "refresh"
    )
    scheduler.add_task(new_task)
