# Install dependencies as needed:
# pip install kagglehub

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from bs4 import BeautifulSoup
import requests, re, json, os, random
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from utils.vehicle_details_helper import (
    get_car_model, get_car_model_html, get_car_brand, get_car_color,
    get_fuel_type, get_price, get_price_html, get_depreciation,
    get_depreciation_soup, get_reg_date, get_mileage, get_mileage_html,
    get_manufactured_year, get_manufactured_year_soup, helper_coe_clean,
    get_coe_left, get_road_tax, get_road_tax_soup, get_transmission,
    get_transmission_soup, get_dereg_value, get_dereg_value_soup,
    get_omv, get_omv_soup, get_coe_value, get_coe_value_soup,
    get_arf, get_arf_soup, get_engine_capacity, get_power, get_power_soup,
    get_curb_weight, get_curb_weight_soup, get_number_of_owners,
    get_number_of_owners_soup, get_type_of_vehicle, get_type_of_vehicle_html,
    get_posted_date, get_last_updated_date, safe_extract)
from utils import google_cloud
from google.cloud import bigquery

# ----------------------------- Google Cloud -----------------------------
GCP_PROJECT_ID = 'is3107-453814'
BQ_DATASET_ID = 'car_dataset'
BQ_TABLE_ID = 'used_car'
BUCKET_NAME = 'is3107-bucket'

# ----------------------------- DAG -----------------------------

# Default Args for DAG
default_args = {
    "owner": "Leng Jin De Joel",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
    

@dag(
    dag_id="SGCarMart_ETL_DAG",
    default_args=default_args,
    description="Extract and process SGCarMart data weekly, through BeautifulSoup and Selenium",
    schedule_interval="@weekly",  # Run Weekly
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["IS3107_GP20_DAG"],
)
def sgcarmart_dag():
    @task
    def get_sgcarmart_data(): # Task ID is the function name
        """
        This task responsible for extracting relevant links in which we will obtain our data from.
        It consists of the following steps:
        1. Set each page to display 100 results and loop for 100 pages
        2. Append each page's URL to a list
        3. Set up Selenium WebDriver
        4. Use Selenium to load the JavaScript and obtain the links of the vehicles in each base URL.
        5. Obtain the posted date of the vehicle listing
        6. For each vehicle link, store the link and posted date in a dictionary
        7. Close the driver
        8. Return the dictionary

        Returns:
            - base_url_dict : dict  -> A dictionary with the 
                                        key as the URL of the main listing and the 
                                        value as a dictionary of the vehicle links and their posted dates.
                                    {base_url_1: {vehicle_link_1: posted_date_1, ...}, ...}
        """

        try:
            main_page_listing_list = [] # creating list to store search pages of 100 car listings
            for i in (range(10)):
                url = "https://www.sgcarmart.com/used_cars/listing.php?BRSR=" + str(i * 100) + "&RPG=100"
                # url = "https://www.sgcarmart.com/used_cars/listing.php?BRSR=100&RPG=100"
                # url = "https://www.sgcarmart.com/used_cars/listing.php?BRSR=" + str(i * 10) + "&RPG=5"
                main_page_listing_list.append(url)
            print(f"Listing list: {main_page_listing_list}")
            # Set up Selenium WebDriver
            options = webdriver.ChromeOptions()
            options.add_argument("--headless")  # Run in headless mode for faster execution
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")

            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

            base_url_dict = {}
            for base_url in main_page_listing_list:
                listing_urls = set()
                driver.get(base_url)
                time.sleep(5)  # Wait for JavaScript to load

                soup = BeautifulSoup(driver.page_source, 'lxml')

                links = soup.find_all('a', class_='styles_text_link__wBaHL') # Obtaining all vehicle links in the page
                posted_date_div = soup.find_all("div", class_="styles_posted_date__ObxTu") # Obtain the posted date of the vehicle listing

                posted_dates = [posted_date_div.text.strip().replace("Posted", "").strip() for posted_date_div in posted_date_div]

                # Filter out car listing links
                for i in range(len(links)):
                    link = links[i]
                    href = link['href']
                    if 'ID=' in link.get('href') and 'DL=' in link.get('href'):
                        full_url = f"https://www.sgcarmart.com{href}" if href.startswith('/') else href
                        listing_urls.add(full_url)

                temp_dict = dict(zip(listing_urls, posted_dates))
                base_url_dict[base_url] = temp_dict

            # Close the driver
            driver.quit()

            return base_url_dict # {base_url_1: {vehicle_link_1: posted_date_1, ...}, ...}
        except Exception as e:
            raise RuntimeError(f"Error downloading dataset: {str(e)}")
    
    @task
    def filter_vehicle_listing(base_url_dict: dict):
        """
            Takes in the upstream base_url_dict and filters for vehicle listings that are posted after a certain date.
            
            Returns:
                - filtered_listings : dict
        """
        client = bigquery.Client()
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"

        query = f"SELECT MAX(posted_datetime) AS latest_posted_date FROM `{table_id}`"
        query_job = client.query(query)
        result = query_job.result()
        latest_posted_date = next(result, None) 

        if latest_posted_date and latest_posted_date.latest_posted_date:
            cutoff_date = latest_posted_date.latest_posted_date
            print(f'Latest posted date from BigQuery: {cutoff_date}')
        else:
            cutoff_date = datetime(2000, 1, 1).date() # Default to a very old date if no data is found
        # cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d") # Replace with database query for latest date available
        filtered_listings = {}

        for base_url, listings in base_url_dict.items():
            print(base_url)
            filtered_listings[base_url] = []

            for vehicle_link, posted_date in listings.items():
                posted_date = datetime.strptime(posted_date, "%d-%b-%Y").date() 
                if posted_date > cutoff_date:
                    filtered_listings[base_url].append(vehicle_link)

        expand_kwargs_list = [
                {"base_url": base_url, "vehicle_links": details} for base_url, details in filtered_listings.items()
        ]
        return expand_kwargs_list
    
    @task
    def extract_vehicle_details(base_url, vehicle_links):
        try:
            #"https://www.sgcarmart.com/used_cars/listing.php?BRSR=" + str(i * 100) + "&RPG=100"
            brsr = base_url.split('BRSR=')[1].split('&')[0]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = os.path.join(os.getcwd(), "output")
            os.makedirs(output_dir, exist_ok=True)

            file = os.path.join(output_dir, f"vehicle_details_{brsr}_{timestamp}.csv")
            
            df = pd.DataFrame(columns=['used_car_id', 'listing_url', 'car_model','brand', 'color', 'fuel_type', 'price',
                           'depreciation_per_year', 'registration_date', 'coe_left', 'mileage', 'manufactured_year',
                           'road_tax_per_year', 'transmission', 'dereg_value', 'omv', 'coe_value', 'arf',
                            'engine_capacity_cc', 'power', 'curb_weight', 'no_of_owners', 'vehicle_type', 
                            'scraped_datetime', 'posted_datetime', 'updated_datetime', 'active'])
            i = 0 

            for listingurl in vehicle_links:
                # listingurl='https://www.sgcarmart.com/used_cars/info.php?ID=1366938&DL=4573&GASRC=dy'
                car_id = listingurl.split("ID=")[1].split("&")[0]

                response = requests.get(listingurl)
                listing_url = BeautifulSoup(response.text, 'lxml')
                soup = BeautifulSoup(response.text, "html.parser")
                
                script_tag = soup.find("script", string=re.compile(r'@context'))
                json_data = np.nan

                # If script_tag with @context is found, parse its content
                if script_tag:
                    try:
                        json_text = script_tag.string.strip()
                        json_data = json.loads(json_text)
                    except json.JSONDecodeError:
                        pass 

                # If no @context script tag is found, check for window._loopaData as fallback
                if json_data is np.nan:
                    script_tag = soup.find("script", text=lambda t: t and 'window._loopaData' in t)
                    if script_tag:
                        try:
                            # Extract the JSON from the window._loopaData assignment
                            json_text = script_tag.string.split('window._loopaData = ')[1].split(';')[0]
                            json_data = json.loads(json_text)
                        except json.JSONDecodeError:
                            pass

                # Extract details using JSON if available; otherwise, fallback to HTML parsing
                car_model = safe_extract(get_car_model, json_data) or safe_extract(get_car_model_html, listing_url)
                brand_name = safe_extract(get_car_brand, json_data) or json_data.get('make')
                color = safe_extract(get_car_color, json_data)
                fuel_type = safe_extract(get_fuel_type, json_data)
                price = safe_extract(get_price, json_data) or safe_extract(get_price_html, listing_url)
                depreciation = safe_extract(get_depreciation, listing_url)
                reg_date = safe_extract(get_reg_date, listing_url)
                coe_remaining = safe_extract(get_coe_left, listing_url)
                mileage = safe_extract(get_mileage, json_data) or safe_extract(get_mileage_html, listing_url)
                manu_year = safe_extract(get_manufactured_year_soup, soup) or safe_extract(get_manufactured_year, listing_url) 
                road_tax = safe_extract(get_road_tax_soup, soup) or safe_extract(get_road_tax, listing_url)
                transmission = safe_extract(get_transmission_soup, soup) or safe_extract(get_transmission, listing_url)
                dereg_value = safe_extract(get_dereg_value_soup, soup) or safe_extract(get_dereg_value, listing_url)
                omv = safe_extract(get_omv_soup, soup) or safe_extract(get_omv, listing_url)
                coe_value = safe_extract(get_coe_value_soup, soup) or safe_extract(get_coe_value, listing_url)
                arf = safe_extract(get_arf_soup, soup) or safe_extract(get_arf, listing_url) 
                engine_capacity = np.nan if fuel_type == "Electric" else safe_extract(get_engine_capacity, listing_url)
                power = safe_extract(get_power_soup, soup) or safe_extract(get_power, listing_url)
                curb_weight = safe_extract(get_curb_weight_soup, soup) or safe_extract(get_curb_weight, listing_url)
                no_of_owners = safe_extract(get_number_of_owners_soup, soup) or safe_extract(get_number_of_owners, listing_url)
                vehicle_type = safe_extract(get_type_of_vehicle, json_data) or safe_extract(get_type_of_vehicle_html, listing_url)
                date_posted = safe_extract(get_posted_date, soup)
                last_updated = safe_extract(get_last_updated_date, soup)

                print(f"Used Car ID: {car_id}")
                print(f'Listing URL: {listingurl}')
                print(f"Car Model: {car_model}")
                print(f"Brand: {brand_name}")
                print(f"Color: {color}")
                print(f"Fuel Type: {fuel_type}")
                print(f"Price: {price} SGD")
                print(f"Depreciation: {depreciation} SGD")
                print(f"Registration Date: {reg_date}")
                print(f"COE Left: {coe_remaining} days")
                print(f"Mileage: {mileage} km")
                print(f"Manufactured: {manu_year}")
                print(f"Road Tax: {road_tax} SGD")
                print(f"Transmission: {transmission}")
                print(f"Dereg Value: {dereg_value} SGD")
                print(f"OMV: {omv} SGD")
                print(f"COE Value: {coe_value} SGD")
                print(f"ARF: {arf} SGD")
                print(f"Engine Capacity: {engine_capacity} cc")
                print(f"Power: {power} kW")
                print(f"Curb Weight: {curb_weight} kg")
                print(f"No. of Owners: {no_of_owners}")
                print(f"Vehicle Type: {vehicle_type}")
                print(f"Date Posted: {date_posted}")
                print(f"Last Updated: {last_updated}")

                df.loc[i, 'used_car_id'] = car_id # INTEGER
                df.loc[i, 'listing_url'] = listingurl # STRING
                df.loc[i, 'car_model'] = car_model # STRING
                df.loc[i, 'brand'] = brand_name # STRING
                df.loc[i, 'color'] = color # STRING
                df.loc[i, 'fuel_type'] = fuel_type # STRING
                df.loc[i, 'price'] = price # FLOAT / DECIMAL in SQL => NUMERIC in BigQuery
                df.loc[i, 'depreciation_per_year'] = depreciation # FLOAT / DECIMAL in SQL => NUMERIC in BigQuery
                df.loc[i, 'registration_date'] = datetime.strptime(reg_date, "%d-%b-%Y").date() # DATE
                df.loc[i, 'coe_left'] = coe_remaining # INTEGER
                df.loc[i, 'mileage'] = mileage # INTEGER
                df.loc[i, 'manufactured_year'] = manu_year # INTEGER
                df.loc[i, 'road_tax_per_year'] = road_tax # FLOAT / DECIMAL in SQL => NUMERIC in BigQuery
                df.loc[i, 'transmission'] = transmission # STRING
                df.loc[i, 'dereg_value'] = dereg_value # FLOAT / DECIMAL in SQL => NUMERIC in BigQuery
                df.loc[i, 'omv'] = omv # FLOAT / DECIMAL in SQL => NUMERIC in BigQuery
                df.loc[i, 'coe_value'] = coe_value # FLOAT / DECIMAL in SQL => NUMERIC in BigQuery
                df.loc[i, 'arf'] = arf # FLOAT / DECIMAL in SQL => NUMERIC in BigQuery
                df.loc[i, 'engine_capacity_cc'] = engine_capacity # INTEGER
                df.loc[i, 'power'] = power # INTEGER
                df.loc[i, 'curb_weight'] = curb_weight # INTEGER
                df.loc[i, 'no_of_owners'] = no_of_owners # INTEGER
                df.loc[i, 'vehicle_type'] = vehicle_type # STRING
                df.loc[i, 'scraped_datetime'] = datetime.now() # DATETIME
                df.loc[i, 'posted_datetime'] = datetime.strptime(date_posted, "%d-%b-%Y").date() # DATE
                df.loc[i, 'updated_datetime'] = datetime.strptime(last_updated, "%d-%b-%Y").date() # DATE
                df.loc[i, 'active'] = True

                i += 1 # Allows next car listing to be put into a next row in the dataframe
                time.sleep(random.randint(0, 3))   # Prevents us from getting locked out of the website

            df.to_csv(file, index=False)

            return {f"{base_url}_CSV_FILEPATH": file}
        except Exception as e:
            raise RuntimeError(f"Error: {str(e)}")

    @task
    def ingest_into_bigquery(file_path_dicts: dict):
        """
            Read the CSV file and ingest the data into BigQuery.
            
            Returns:
                - Boolean flag indicating success
        """
        try:
            # file_path_dicts will be a list of dictionaries from the dynamic task mapping
            for file_path_dict in file_path_dicts:
                for key, file_path in file_path_dict.items():
                    print(f"Ingesting file: {file_path} from {key}")

                    # Read the CSV file
                    df = pd.read_csv(file_path)

                    # Convert only in this task because we re-read the CSV file
                    # Convert the following columns to INTEGER type for BigQuery
                    columns_to_convert = ['used_car_id', 'coe_left', 'mileage', 
                                          'manufactured_year', 'engine_capacity_cc', 
                                          'power', 'curb_weight', 'no_of_owners']
                    for col in columns_to_convert:
                        df[col] = np.floor(pd.to_numeric(df[col], errors='coerce')).astype("Int64")


                    # Convert the following columns to FLOAT type for BigQuery
                    df['price'] = df['price'].astype("float")
                    df['depreciation_per_year'] = df['depreciation_per_year'].astype("float")
                    df['road_tax_per_year'] = df['road_tax_per_year'].astype("float")
                    df['dereg_value'] = df['dereg_value'].astype("float")
                    df['omv'] = df['omv'].astype("float")
                    df['coe_value'] = df['coe_value'].astype("float")
                    df['arf'] = df['arf'].astype("float")

                    # Convert the following columns to DATE type for BigQuery
                    df['registration_date'] = pd.to_datetime(df['registration_date'], format="%Y-%m-%d").dt.date
                    df['posted_datetime'] = pd.to_datetime(df['posted_datetime'], format="%Y-%m-%d").dt.date
                    df['updated_datetime'] = pd.to_datetime(df['updated_datetime'], format="%Y-%m-%d").dt.date
                    df['scraped_datetime'] = pd.to_datetime(df['scraped_datetime'], format="%Y-%m-%d %H:%M:%S.%f")

                    # Check the type of each column
                    print(df.dtypes)
                    # Check the type of each cell in the first row
                    print(df.iloc[0].apply(type))

                    """Uploads DataFrame to GCS via google_cloud.py"""
                    df_gcs_uri = google_cloud.upload_to_gcs(df, bucket_name=BUCKET_NAME, prefix=BQ_TABLE_ID)

                    """Loads GCS data into BigQuery via google_cloud.py"""
                    google_cloud.load_to_bigquery(df_gcs_uri, GCP_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID)
                    
            return True
        except Exception as e:
            raise RuntimeError(f"Error: {str(e)}")


    # DAG Node order
    base_url_dict = get_sgcarmart_data()
    vehicle_kwargs_list= filter_vehicle_listing(base_url_dict)
    vehicle_details_filepath = extract_vehicle_details.expand_kwargs(vehicle_kwargs_list)
    ingestion_result = ingest_into_bigquery(vehicle_details_filepath)

# instantiate the DAG
sgcarmart_dag()

##### Testing 
def get_sgcarmart_data(): # Task ID is the function name
    """
    This task responsible for extracting relevant links in which we will obtain our data from.
    It consists of the following steps:
    1. Set each page to display 100 results and loop for 100 pages
    2. Append each page's URL to a list
    3. Set up Selenium WebDriver
    4. Use Selenium to load the JavaScript and obtain the links of the vehicles in each base URL.
    5. Obtain the posted date of the vehicle listing
    6. For each vehicle link, store the link and posted date in a dictionary
    7. Close the driver
    8. Return the dictionary

    Returns:
        - base_url_dict : dict  -> A dictionary with the 
                                    key as the URL of the main listing and the 
                                    value as a dictionary of the vehicle links and their posted dates.
                                {base_url_1: {vehicle_link_1: posted_date_1, ...}, ...}
    """

    try:
        main_page_listing_list = [] # creating list to store search pages of 100 car listings
        for i in (range(1)):
            url = "https://www.sgcarmart.com/used_cars/listing.php?BRSR=" + str(i * 100) + "&RPG=100"
        main_page_listing_list.append(url)

        # Set up Selenium WebDriver
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")  # Run in headless mode for faster execution
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

        base_url_dict = {}
        for base_url in main_page_listing_list:
            listing_urls = set()
            driver.get(base_url)
            time.sleep(3)  # Wait for JavaScript to load

            soup = BeautifulSoup(driver.page_source, 'lxml')

            links = soup.find_all('a', class_='styles_text_link__wBaHL') # Obtaining all vehicle links in the page
            posted_date_div = soup.find_all("div", class_="styles_posted_date__ObxTu") # Obtain the posted date of the vehicle listing

            posted_dates = [posted_date_div.text.strip().replace("Posted", "").strip() for posted_date_div in posted_date_div]

            # Filter out car listing links
            for i in range(len(links)):
                link = links[i]
                href = link['href']
                if 'ID=' in link.get('href') and 'DL=' in link.get('href'):
                    full_url = f"https://www.sgcarmart.com{href}" if href.startswith('/') else href
                    listing_urls.add(full_url)

            temp_dict = dict(zip(listing_urls, posted_dates))
            base_url_dict[base_url] = temp_dict

        # Close the driver
        driver.quit()

        return base_url_dict # {base_url_1: {vehicle_link_1: posted_date_1, ...}, ...}
    except Exception as e:
        raise RuntimeError(f"Error downloading dataset: {str(e)}")


def filter_vehicle_listing(base_url_dict: dict):
    """
        Takes in the upstream base_url_dict and filters for vehicle listings that are posted after a certain date.
        
        Returns:
            - filtered_listings : dict
    """
    cutoff_date = datetime.strptime("2025-03-01", "%Y-%m-%d") # Replace with database query for latest date available
    filtered_listings = {}

    for base_url, listings in base_url_dict.items():
        filtered_listings[base_url] = []

        for vehicle_link, posted_date in listings.items():
            posted_date = datetime.strptime(posted_date, "%d-%b-%Y")
            if posted_date > cutoff_date:
                filtered_listings[base_url].append(vehicle_link)

    return filtered_listings

if __name__=='__main__':
    # transform_sales_data()
    
    x  = get_sgcarmart_data()
    # y = filter_vehicle_listing(x)
    # print(x)
    # print(y)
