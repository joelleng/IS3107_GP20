import re
import json
import datetime
import time
import pandas as pd

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from urllib.parse import urlparse, parse_qs


# Utility Functions
def convert_text_to_float(text):
    if text == 'N.A.':
        return None
    try:
        cleaned_text = re.sub(r'[^\d,\.]+', '', text)
        value = float(cleaned_text.replace(',', ''))
        return value
    except TypeError:
        return None
    except ValueError:
        return None

def convert_text_to_int(text):
    if text == 'N.A.':
        return None
    cleaned_text = re.sub(r'[^\d,\.]+', '', text)
    try:
        value = int(float(cleaned_text.replace(',', '')))
        return value
    except ValueError:
        return None       

def parse_listing_id(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    if 'ID' in query_params and query_params['ID']:
        return int(query_params['ID'][0])
    return None

def parse_reg_date(text):
    date_pattern = r"(\d{1,2}-[A-Za-z]{3}-\d{4})"
    date_match = re.search(date_pattern, text)
    if not date_match:
        return None
    date_str = date_match.group(0)
    date = datetime.datetime.strptime(date_str, "%d-%b-%Y").date()
    return date

def parse_coe_left(text):
    coe_left_pattern = r"\(([^)]*)\)"
    coe_left_match = re.search(coe_left_pattern, text)
    if not coe_left_match:
        return None
    coe_left_str = coe_left_match.group(1)
    years_match = re.search(r"(\d+)\s*yrs", coe_left_str)
    months_match = re.search(r"(\d+)\s*mths", coe_left_str)
    days_match = re.search(r"(\d+)\s*days", coe_left_str)

    yrs = int(years_match.group(1)) if years_match else 0
    mths = int(months_match.group(1)) if months_match else 0
    dys = int(days_match.group(1)) if days_match else 0

    total_days = yrs * 365 + mths * 30 + dys
    return total_days

def parse_json_details(details_tag):
    if not details_tag:
        return {
            "car_model": None,
            "brand": None,
            "price_text": None,
            "color": None,
            "fuel_type": None,
            "manufactured_year": None,
            "transmission": None,
        }
    try:
        details_json = json.loads(details_tag.string)
    except json.JSONDecodeError:
        details_json = {}

    return {
        "car_model": details_json.get('model'),
        "brand": details_json.get('brand', {}).get('name'),
        "price_text": details_json.get('offers', {}).get('price'),
        "color": details_json.get('color'),
        "fuel_type": details_json.get('vehicleEngine', {}).get('fuelType'),
        "manufactured_year": int(details_json.get('vehicleModelDate')),
        "transmission": details_json.get('vehicleTransmission'),
    }

def parse_html_fallback_details(html):
    fallback_data = {} 
    
    # Car model
    car_model_tag = html.find('a', class_='nounderline globaltitle')
    if car_model_tag and car_model_tag.text:
        fallback_data["car_model"] = car_model_tag.text.split("(")[0].strip()
        fallback_data["brand"] = fallback_data["car_model"].split(" ")[0]
    else:
        fallback_data["car_model"] = None
        fallback_data["brand"] = None
    
    # Price
    a_tag = html.find('tr', class_='row_bg').find('a', href=True)
    try:
        if 'info_financial' in a_tag.get('href'):
            fallback_data["price_text"] = a_tag.get_text(strip=True)
        else:
            fallback_data["price_text"] = html.find('tr', class_='row_bg').get_text(strip=True)
    except Exception:
        fallback_data["price_text"] = None
  
    
    # Some fields remain None because they won't be found in fallback HTML
    fallback_data["color"] = None
    fallback_data["fuel_type"] = None
    fallback_data["manufactured_year"] = None
    fallback_data["transmission"] = None
    
    return fallback_data

def parse_table_details(html):
    data = {
        "depreciation": None,
        "registration_date": None,
        "coe_left": None,
        "mileage": None,
        "road_tax_per_year": None,
        "dereg_value": None,
        "omv": None,
        "coe_value": None,
        "arf": None,
        "engine_capacity_cc": None,
        "power": None,
        "curb_weight": None,
        "no_of_owners": None,
        "vehicle_type": None,
        "active": False,
    }
    
    table = html.find('table', id='carInfo')
    if not table:
        return data 

    rows = table.find_all('tr')

    # --- Row[1] parse (Depreciation, RegDate) ---
    if len(rows) > 1:
        labels = rows[1].find_all('strong')
        for label in labels:
            text = label.find_next('td').get_text(" ", strip=True)
            label = label.text.strip()
            if 'Depreciation' in label:
                if text != 'N.A.':
                    data["depreciation"] = convert_text_to_float(text)
            if 'Reg Date' in label:
                data["registration_date"] = parse_reg_date(text)
                data["coe_left"] = parse_coe_left(text)
    
    # --- Row[2] parse (Mileage, Road Tax, etc.) ---
    if len(rows) > 2:
        labels = rows[2].find_all('strong')
        for label in labels:
            text = label.find_next('div', class_='row_info').get_text(" ", strip=True)
            label = label.text.strip()

            if 'Mileage' in label:
                num_part = text.split(" ")[0]
                data["mileage"] = convert_text_to_int(num_part)
            elif 'Road Tax' in label:
                data["road_tax_per_year"] = convert_text_to_float(text)
            elif 'Dereg Value' in label:
                data["dereg_value"] = convert_text_to_float(text)
            elif 'OMV' in label:
                data["omv"] = convert_text_to_float(text)
            elif 'ARF' in label:
                data["arf"] = convert_text_to_float(text)
            elif 'Engine Cap' in label:
                data["engine_capacity_cc"] = convert_text_to_int(text)
            elif 'Power' in label:
                cleaned_text = text.split(" ")[0]
                data["power"] = convert_text_to_int(cleaned_text)
            elif 'Curb Weight' in label:
                data["curb_weight"] = convert_text_to_int(text)
            elif 'No. of Owners' in label:
                data["no_of_owners"] = convert_text_to_int(text)
            elif 'Type of Vehicle' in label:
                data["vehicle_type"] = text

        # COE
        coe = rows[2].find('a', href='popups/whatsCOE.php')
        if coe:
            coe_text = coe.find_next('div', class_='row_info').get_text(" ", strip=True)
            data["coe_value"] = convert_text_to_float(coe_text)

    # --- Possibly row[3] parse (Vehicle Type fallback) ---
    if data["vehicle_type"] is None and len(rows) > 3:
        # If row[3] is some fallback for type
        vehicle_type_tag = rows[3].find('a')
        if vehicle_type_tag:
            data["vehicle_type"] = vehicle_type_tag.get_text(" ", strip=True)

    # --- Parse (Status) ---
    status_tag = table.find('strong', string='Status')
    if status_tag:
        status_text = status_tag.find_next('span').get_text(strip=True)
        print("status text: ", status_text)
        if 'Available for sale' in status_text:
            data["active"] = True

    return data

def parse_listing_metadata(html):
    posted_date = None
    updated_date = None

    text = html.find('div', id='usedcar_postdate').get_text()
    pattern = re.compile(
    r"Posted on:\s*(\d{1,2}-[A-Za-z]{3}-\d{4}).*?"
    r"Last\s+Updated on:\s*(\d{1,2}-[A-Za-z]{3}-\d{4})",
    re.DOTALL
    )
    match = pattern.search(text)
    if match:
        posted_str = match.group(1)
        print("match 1", match.group(1))
        print("match 2", match.group(2))
        updated_str = match.group(2)

        posted_date = datetime.datetime.strptime(posted_str, "%d-%b-%Y").date()
        updated_date = datetime.datetime.strptime(updated_str, "%d-%b-%Y").date()
    
    return posted_date, updated_date

def scrape_indiv_listing(listing_url, driver): 
    driver.get(listing_url)
    content = driver.page_source
    html = BeautifulSoup(content, 'lxml')

    """
    Flow: 
      1) Load page
      2) Find JSON-LD details or fallback HTML
      3) Parse table details
      4) Return row_data dict
    """

    # -- 1) Extract listing id ---
    used_car_id = parse_listing_id(listing_url)

    # --- 2) Attempt JSON parse ---
    details_tag = html.find('script', type='application/ld+json')
    json_info = parse_json_details(details_tag)

    # --- 3) If JSON is missing, fallback to HTML parse ---
    if json_info["car_model"] is None:
        fallback_info = parse_html_fallback_details(html)
        for key, val in fallback_info.items():
            if json_info.get(key) is None:
                json_info[key] = val

    # --- 4) Convert price_text to float if not None ---
    price_text = json_info["price_text"]
    price = convert_text_to_float(price_text)

    # --- 5) Parse table details (Depreciation, RegDate, etc.) ---
    table_data = parse_table_details(html)

    # --- 6) Extract posted and last updated date ---
    posted_date, updated_date = parse_listing_metadata(html)

    # --- 7) Assemble final row_data ---
    row_data = {
        'used_car_id': used_car_id, 
        'listing_url': listing_url,
        'car_model': json_info["car_model"],
        'brand': json_info["brand"],
        'color': json_info["color"],
        'fuel_type': json_info["fuel_type"],
        'price': price,
        'depreciation_per_year': table_data["depreciation"],
        'registration_date': table_data["registration_date"],
        'coe_left': table_data["coe_left"],
        'mileage': table_data["mileage"],
        'manufactured_year': json_info["manufactured_year"],
        'road_tax_per_year': table_data["road_tax_per_year"],
        'transmission': json_info["transmission"],
        'dereg_value': table_data["dereg_value"],
        'omv': table_data["omv"],
        'coe_value': table_data["coe_value"],
        'arf': table_data["arf"],
        'engine_capacity_cc': table_data["engine_capacity_cc"],
        'power': table_data["power"],
        'curb_weight': table_data["curb_weight"],
        'no_of_owners': table_data["no_of_owners"],
        'vehicle_type': table_data["vehicle_type"],
        'scraped_datetime' : datetime.datetime.now(),
        'posted_datetime': posted_date,
        'updated_datetime': updated_date,
        'active': table_data["active"],
    }

    return row_data


# Scrape Function
def scrape(page):
    cars = []
    url = 'https://www.sgcarmart.com/used-cars/listing?avl=&limit=50'

    # =============== Set up Selenium driver =============== 
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--window-size=2000,1080") # to make window large enough to show listing post date
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)

    # =============== Start scraping through pages =============== 
    print(f"=== Page: {page}")

    if (page > 1):
        driver.get(url + f'&page={page}')  
    else:
        driver.get(url)

    WebDriverWait(driver, 20).until(
        EC.text_to_be_present_in_element((By.CSS_SELECTOR, "div.styles_posted_date__ObxTu"), "Posted")
    ) # to ensure page is fully loaded
    content = driver.page_source
    html = BeautifulSoup(content, 'lxml')

    # Find out the latest listing in a page
    date_string = html.find('div', class_='styles_posted_date__ObxTu').get_text(strip=True)   
    date_string = date_string.replace('Posted ', '')
    latest_date = datetime.datetime.strptime(date_string, "%d-%b-%Y").date()
    print(f"--- Latest Listing Date: {latest_date}")

    if latest_date < datetime.date(2025, 1, 1):
        print("=== Scrape Ended ===")
        driver.quit()
        return

    # Loop through all listings in a page 
    raw_el = html.find_all('a', class_='styles_text_link__wBaHL', href=True)
    listings = [el['href'] for el in raw_el[::2]] # to skip repeated links
    for i, listing in enumerate(listings, start=1):
        print(f"--- {i}: {listing}")
        car_data = scrape_indiv_listing(listing, driver)
        cars.append(car_data)
        time.sleep(1)

    df = pd.DataFrame(cars)
    df["mileage"] = df["mileage"].astype("Int64")
    df["manufactured_year"] = df["manufactured_year"].astype("Int64")
    df["coe_left"] = df["coe_left"].astype("Int64")
    df["engine_capacity_cc"] = df["engine_capacity_cc"].astype("Int64")
    df["power"] = df["power"].astype("Int64")
    df["curb_weight"] = df["curb_weight"].astype("Int64")
    df["no_of_owners"] = df["no_of_owners"].astype("Int64")
    print("=== Scrape Completed ===")
    
    driver.quit()
    return df



# need to start again from page 16




