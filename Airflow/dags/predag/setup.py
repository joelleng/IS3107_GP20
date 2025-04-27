import re
import datetime
import time

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from utils import google_cloud
from predag import usedcars_scraper


def main():
    url = 'https://www.sgcarmart.com/used-cars/listing?avl=&limit=50'

    # =============== Set up Selenium driver =============== 
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--window-size=2000,1080") # to make window large enough to show listing post date
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    WebDriverWait(driver, 20).until(
        EC.text_to_be_present_in_element((By.CSS_SELECTOR, "div.styles_posted_date__ObxTu"), "Posted")
    ) # to ensure page is fully loaded

    content = driver.page_source
    html = BeautifulSoup(content, 'lxml')

    # =============== Print scrape job summary =============== 
    current_date = datetime.date.today()

    # Find the total no. of listings
    summary = html.find('div', class_='styles_result_count__tWt03')
    num_string = re.sub(r'[^\d]', '', summary.text) # to extract no. of listings from summary text
    num_listings = int(num_string)

    # Find the total no. of pages
    pagination = html.find('ul', class_='styles_pagination_container__UnHJ0 m-0 pagination')
    pages = pagination.find_all('li') # to get the last page number in pagination control
    num_pages = int(pages[-1].text)

    # Find the posting date of the lastest listing
    date_string = html.find('div', class_='styles_posted_date__ObxTu').get_text(strip=True)   
    date_string = date_string.replace('Posted ', '')
    latest_date = datetime.datetime.strptime(date_string, "%d-%b-%Y").date()

    # Print
    print(f"=== Scrape Started ===")
    print(f"Date: {current_date}")
    print(f"Lastest Listing Date: {latest_date}")
    print(f"Total Listings: {num_listings}")
    print(f"Total Pages: {num_pages}")
    print(f"======================")

    for page in range(1, num_pages + 1):
        df = usedcars_scraper.scrape(page)
        gcs_uri = google_cloud.upload_to_gcs(df, bucket_name='is3107-bucket', prefix='used_car')
        google_cloud.load_to_bigquery(gcs_uri, 
                                        gcp_project_id='is3107-453814',
                                        bq_dataset_id='car_dataset',
                                        bq_table_id='used_car')
        time.sleep(1)

if __name__ == "__main__":
    main()

















