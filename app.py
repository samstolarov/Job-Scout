import boto3
import streamlit as st
import requests

st.set_page_config(page_title="JobScout", layout="wide")

st.title("JobScout")
st.write(
    "JobScout is a web application that simplifies job searching by querying multiple job listing sites and saving "
    "user-defined searches. Utilizing a distributed work scheduler, it regularly updates the job listings database "
    "and notifies users of new opportunities. The application also features a user-friendly web interface for "
    "manual queries and real-time results."
)

job_title = st.text_input("Job Title:")
states = ['', 'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut',
          'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa',
          'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan',
          'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire',
          'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio',
          'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
          'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia',
          'Wisconsin', 'Wyoming']
location = st.selectbox(label="Location:", options=states)
company = st.text_input("Company:")

frequency = ['', 'One-Time Instant Results', 'Every Minute (For Testing)', 'Daily', 'Biweekly', 'Weekly', 'Bimonthly', 'Monthly']
frequencies = st.selectbox(label="How often would you like to be notified?:", options=frequency)

login_url = "http://ec2-18-191-83-191.us-east-2.compute.amazonaws.com:8080/login"
logout_url = "http://ec2-18-191-83-191.us-east-2.compute.amazonaws.com:8080/logout"
is_logged_in_url = "http://ec2-18-191-83-191.us-east-2.compute.amazonaws.com:8080/is_logged_in"



if 'button_login_pressed' not in st.session_state:
    st.session_state.button_login_pressed = False

# Function to check login status
def check_login_status():
    response = requests.get(is_logged_in_url)
    if response.status_code == 200:
        data = response.json()
        st.session_state.button_login_pressed = data['logged_in']

# Function to handle login press
def handle_button_login_press():
    st.session_state.button_login_pressed = True
    st.markdown(f'<meta http-equiv="refresh" content="0; url={login_url}">', unsafe_allow_html=True)
    check_login_status()

# Function to handle logout press
def handle_button_logout_press():
    st.session_state.button_login_pressed = False
    st.markdown(f'<meta http-equiv="refresh" content="0; url={logout_url}">', unsafe_allow_html=True)

# Sidebar for floating menu
with st.sidebar:
    st.header("Menu")
    # Display buttons based on the session state
    if not st.session_state.button_login_pressed:
        if st.button("Login with Google"):
            handle_button_login_press()
    else:
        if st.button("Logout"):
            handle_button_logout_press()

def convert_frequency_to_interval(frequency) -> str:
        if frequency == 'Every Minute (For Testing)':
            return "PT1M"
        elif frequency == 'Daily':
            return "P1D"
        elif frequency == 'Weekly':
            return "P7D"
        elif frequency == 'Bimonthly':
            return "P14D"
        elif frequency == 'Monthly':
            return "P30D"
        elif frequency == 'Biweekly':
            return "P3.5D"
        else:
            return "P7D"

if st.button("Search"):
    if job_title or location or company:
        if frequencies == 'One-Time Instant Results':
            job_search_data = {
            'title': job_title,
            'company': company,
            'location': location
            }
            try:
                # Build the URL with query parameters
                response = requests.get(
                    'http://ec2-18-191-83-191.us-east-2.compute.amazonaws.com:8000/instant_search/',
                    params={
                        'role': job_title,  # Assuming job_title is used as role
                        'location': location,
                        'company': company
                    }
                )

                if response.status_code == 200:
                    # Parse the JSON response
                    results = response.json().get('results', [])
                    if not results:
                        st.write("No results found.")
                    
                    # Display the results in a formatted way
                    st.success('Search request sent! Check your results below:')
                    
                    for job in results:
                        title = job.get('title', 'N/A')
                        company = job.get('company', 'N/A')
                        location = job.get('location', 'N/A')
                        link = job.get('link', '#')
                        if title == 'No matching jobs found.':
                            st.write("Sorry, no matching jobs found. Please try another search.")
                        else:
                            st.write(f"**Title:** {title}")
                            st.write(f"**Company:** {company}")
                            st.write(f"**Location:** {location}")
                            st.markdown(f"[Link to Apply]({link})")  # Clickable link
                            st.write("---")  # Separator between jobs

                else:
                    st.error(f"Failed to add job search. Error: {response.text}")
            except Exception as e:
                st.error(f"An error occurred: {e}")
        elif frequencies:
            st.error("Please sign in to save a search. If you'd like to conduct an instant search, please select One-Time Instant Results")
        else:
            st.error("Please choose a notification setting.")
    else:
        st.error("Please fill out a field before searching.")

st.text("")
st.text("")
st.text("")
st.text("")
st.text("")
st.info("The following is for demo purposes and still needs to be integrated with the rest of the application:")
st.title("Display all Jobs")
if st.button("Jobs"):
    # Initialize a session using Amazon DynamoDB
    session = boto3.Session(
        region_name='us-east-2'  # Specify the region
    )

    # Initialize DynamoDB resource
    dynamodb = session.resource('dynamodb')

    # Specify the table
    table = dynamodb.Table('Jobs')

    # Scan the table to get all items
    response = table.scan()
    jobs = response.get('Items', [])

    for job in jobs:
        st.write(f"**Company:** {job.get('company', 'N/A')}")
        st.write(f"**Description:** {job.get('description', 'N/A')}")
        st.write(f"**Location:** {job.get('location', 'N/A')}")
        link = job.get('link', '#')
        st.markdown(f"[Apply Here]({link})")
        st.write("---")

# # AWS DynamoDB configuration
# dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
# table = dynamodb.Table('tasks')
#
# def fetch_searches():
#     try:
#         response = table.scan()
#         return response.get('Items', [])
#     except (NoCredentialsError, PartialCredentialsError) as e:
#         st.error("AWS credentials not found.")
#         return []
#
# def display_searches():
#     searches = fetch_searches()
#     if not searches:
#         st.write("No searches found.")
#     else:
#         search_dict = {}
#         count = 1
#         for search in searches:
#             company = search.get('company', 'N/A')
#             location = search.get('location', 'N/A')
#             title = search.get('title', 'N/A')
#             # Check if the fields are empty and set to 'N/A' if they are
#             company = company if company else 'N/A'
#             location = location if location else 'N/A'
#             title = title if title else 'N/A'
#             search_dict[count] = {
#                 'company': company,
#                 'location': location,
#                 'title': title
#             }
#             count += 1
#
#         # Display the searches using Streamlit
#         for key, value in search_dict.items():
#             st.write(f"({key}) Company: {value['company']}, Location: {value['location']}, Title: {value['title']}")
#
# if st.button('Dsiplay My Searches'):
#     display_searches()