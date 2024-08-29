# import csv
# import requests
# from bs4 import BeautifulSoup
#
# file = open('linkedin-jobs.csv', 'a')
# writer = csv.writer(file)
# writer.writerow(['Title', 'Company', 'Location', 'Apply'])
#
#
# def linkedin_scraper(webpage, page_number):
#     next_page = webpage + str(page_number)
#     print(str(next_page))
#
#     response = requests.get(str(next_page))
#
#     soup = BeautifulSoup(response.content, 'html.parser')
#
#     jobs = soup.find_all('div', class_='base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card')
#
#     for job in jobs:
#         job_title = job.find('h3', class_='base-search-card__title').text.strip()
#         job_company = job.find('h4', class_='base-search-card__subtitle').text.strip()
#         job_location = job.find('span', class_='job-search-card__location').text.strip()
#         job_link = job.find('a', class_='base-card__full-link')['href']
#
#     writer.writerow([
#         job_title.encode('utf-8'),
#         job_company.encode('utf-8'),
#         job_location.encode('utf-8'),
#         job_link.encode('utf-8')
#     ])
#
#     print('Data updated')
#
#     if page_number < 300:
#         page_number = page_number + 25
#         linkedin_scraper(webpage, page_number)
#     else:
#         file.close()
#         print('File closed')
# linkedin_scraper('https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=Product%20Management&location=San%20Francisco%20Bay%20Area&geoId=90000084&trk=public_jobs_jobs-search-bar_search-submit&position=1&pageNum=0&start=',0)
#

import csv
import requests
from bs4 import BeautifulSoup
import time

file = open('linkedin-jobs.csv', 'a', newline='', encoding='utf-8')
writer = csv.writer(file)
writer.writerow(['Title', 'Company', 'Location', 'Apply'])

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}


def linkedin_scraper(webpage, page_number):
    next_page = webpage + str(page_number)
    print(f"Scraping URL: {next_page}")

    response = requests.get(next_page, headers=headers)

    if response.status_code != 200:
        print(f"Failed to retrieve page {page_number}. Status code: {response.status_code}")
        return

    soup = BeautifulSoup(response.content, 'html.parser')

    jobs = soup.find_all('div',
                         class_='base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card')

    if not jobs:
        print(f"No jobs found on page {page_number}. Ending scrape.")
        return

    for job in jobs:
        job_title = job.find('h3', class_='base-search-card__title').text.strip()
        job_company = job.find('h4', class_='base-search-card__subtitle').text.strip()
        job_location = job.find('span', class_='job-search-card__location').text.strip()
        job_link = job.find('a', class_='base-card__full-link')['href']

        writer.writerow([
            job_title,
            job_company,
            job_location,
            job_link
        ])

    print(f"Data updated with {len(jobs)} jobs from page {page_number}")

    if len(jobs) == 0 or page_number >= 200:
        file.close()
        print('File closed')
        return

    # Adding a delay to avoid hitting the server too quickly
    # time.sleep(1)

    linkedin_scraper(webpage, page_number + 25)


linkedin_scraper(
    'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=Product%20Management&location=San%20Francisco%20Bay%20Area&geoId=90000084&trk=public_jobs_jobs-search-bar_search-submit&position=1&pageNum=0&start=',
    0)