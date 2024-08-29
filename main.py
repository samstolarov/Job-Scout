import datetime

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse
import requests
import threading
import subprocess
import uvicorn
import logging
import notifs
import refresh
import os
from pydantic import BaseModel
from task_sched_dbs.Master import Master
from task_sched_dbs.Tables import Notifs, Task, Refresh
from flask_application import app as flask_app
from scraper import Scraper
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# OAuth configuration
client_id = os.getenv('GOOGLE_CLIENT_ID', 'your_default_client_id')
client_secret = os.getenv('GOOGLE_CLIENT_SECRET', 'your_default_client_secret')
redirect_uri = 'http://ec2-3-21-189-151.us-east-2.compute.amazonaws.com:8080/callback'

# Initialize FastAPI app
app = FastAPI()
master = Master(10)
scraper = Scraper()
scraper.linkedin_scraper()

new_task = Refresh(
    task_id=0,
    interval="PT6H",
    retries=3,
    created=int(datetime.now().timestamp()),
    last_refresh=0,
    type="refresh"
)
master.add_task(new_task)


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

@app.post("/add_search/")
def add_job_search(job_search: Notifs):
    try:
        task_id = master.add_task(job_search)
        return {"task_id": task_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/instant_search/")
def scrape_jobs(role: str, location: str, company: str):
    try:
        results = notifs.perform_search(role, location, company)
        print(results)
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/login")
def login(request: Request):
    google_oauth_url = (
        "https://accounts.google.com/o/oauth2/auth"
        "?response_type=code"
        "&client_id={client_id}"
        "&redirect_uri={redirect_uri}"
        "&scope=openid%20email%20profile"
        "&access_type=offline"
    ).format(client_id=client_id, redirect_uri=redirect_uri)
    logging.info(f"Redirecting to Google OAuth URL: {google_oauth_url}")
    return RedirectResponse(google_oauth_url)

@app.get("/callback")
def callback(code: str, request: Request):
    logging.info(f"Received callback with code: {code}")
    token_url = "https://oauth2.googleapis.com/token"
    token_data = {
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    }

    try:
        token_r = requests.post(token_url, data=token_data)
        token_r.raise_for_status()
        token_json = token_r.json()
        id_token = token_json.get("id_token")
        access_token = token_json.get("access_token")
        logging.info(f"Token response: {token_json}")

        userinfo_url = "https://www.googleapis.com/oauth2/v1/userinfo"
        userinfo_params = {"access_token": access_token}
        userinfo_response = requests.get(userinfo_url, params=userinfo_params)
        userinfo_response.raise_for_status()
        userinfo = userinfo_response.json()

        logging.info(userinfo)
        
        # Redirect to logged-in application
        return RedirectResponse("http://ec2-3-21-189-151.us-east-2.compute.amazonaws.com:8502")  # Ensure this URL points to logged_in_app.py
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during token exchange: {e}")
        return {"error": str(e)}

@app.get("/is_logged_in")
def is_logged_in():
    # Implement a check for login status
    # For now, just return a dummy response
    return {"status": "success", "name": "John Doe"}

def run_fastapi():
    logging.info('Starting FastAPI on port 8000')
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")

def run_flask():
    logging.info('Starting Flask app on port 8080')
    flask_app.run(host="0.0.0.0", port=8080)

def run_logged_in_app():
    subprocess.Popen(['streamlit', 'run', 'logged_in_app.py', '--server.port', '8502'])

def run_streamlit():
    subprocess.Popen(['streamlit', 'run', 'app.py', '--server.port', '8501'])

def start_sqs_listener(process_message_function, thread_name):
    while True:
        process_message_function()

if __name__ == "__main__":
    fastapi_thread = threading.Thread(target=run_fastapi)
    flask_thread = threading.Thread(target=run_flask)
    streamlit_thread = threading.Thread(target=run_streamlit)
    logged_in_app_thread = threading.Thread(target=run_logged_in_app)

    # SQS listener threads
    refresh_listener_thread = threading.Thread(target=start_sqs_listener, args=(refresh.process_refresh_message, 'refresh_listener'))
    notifs_listener_thread = threading.Thread(target=start_sqs_listener, args=(notifs.process_notifs_message, 'notifs_listener'))

    fastapi_thread.start()
    flask_thread.start()
    streamlit_thread.start()
    logged_in_app_thread.start()
    #refresh_listener_thread.start()
    notifs_listener_thread.start()

    fastapi_thread.join()
    flask_thread.join()
    streamlit_thread.join()
    logged_in_app_thread.join()
    #refresh_listener_thread.join()
    notifs_listener_thread.join()
