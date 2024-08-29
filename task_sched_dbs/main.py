from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import RedirectResponse, HTMLResponse
from flask import Flask, redirect, url_for
import requests
import threading
import uvicorn
import os
import sys
from task_sched_dbs.Master import Master
from task_sched_dbs.Tables import Notifs, Task
from pydantic import BaseModel

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Now you can import flask_application
from flask_application import app as flask_app

flask_app.config['GOOGLE_ID'] = '197014094036-rbrpc7ot7nmkkj401809qbb1nheakeis.apps.googleusercontent.com'
flask_app.config['GOOGLE_SECRET'] = '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'
flask_app.config['GOOGLE_REDIRECT_URI'] = 'http://localhost:5000/callback'

app = FastAPI()
master = Master(18)

# Start the master scheduler in the background
master_thread = threading.Thread(target=master.run, daemon=True)
master_thread.start()

class TaskUpdateRequest(BaseModel):
    task_id: int
    new_task: Task

@app.delete("/delete_task/{task_id}")
def delete_task(task_id: int):
    result = master.delete_task(task_id)
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result["message"])
    return result

# @app.put("/update_task/{task_id}")
# def update_task(task_id: int, new_task: Task):
#     try: 
#         task = new_task.dict()
#         if master.get_task(task_id).type == 'notif':
#             result = master.update_task(task_id, task)
#             if result["status"] == "error":
#                 raise HTTPException(status_code=500, detail=result["message"])
#             return result
#     except Exception as e: 
#         raise Exception(f"failed to update task with id {task_id}: {e}")


@app.post("/add_search/")
def add_job_search(job_search: Notifs):
    try:
        task_id = master.add_task(job_search)
        return {"task_id": task_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/instant_search/")
def scrape_jobs():
    return master.scrape_jobs()



def run_fastapi():
    uvicorn.run(app, host="0.0.0.0", port=8000)

def run_streamlit_logged_in():
    os.system('streamlit run ../logged_in_app.py')

def run_streamlit_logged_out():
    os.system('streamlit run ../app.py')

if __name__ == "__main__":
    fastapi_thread = threading.Thread(target=run_fastapi)
    streamlit_thread_one = threading.Thread(target=run_streamlit_logged_out)
    streamlit_thread_two = threading.Thread(target=run_streamlit_logged_in)

    fastapi_thread.start()
    streamlit_thread_one.start()
    streamlit_thread_two.start()

    fastapi_thread.join()
    streamlit_thread_one.join()
    streamlit_thread_two.join()