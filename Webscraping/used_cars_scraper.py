import re
import json
import datetime
import pandas as pd

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


# Utility Functions
def convert_text_to_float(text):
    if text == 'N.A.':
        return None
    cleaned_text = re.sub(r'[^\d,\.]+', '', text)
    value = float(cleaned_text.replace(',', ''))
    return value

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

def parse_json_details(details_tag, html):
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
        "manufactured_year": details_json.get('vehicleModelDate'),
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
    row_bg = html.find('tr', class_='row_bg')
    if row_bg:
        fallback_data["price_text"] = row_bg.get_text(strip=True)
    else:
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
                data["mileage"] = convert_text_to_float(num_part)
            elif 'Road Tax' in label:
                data["road_tax_per_year"] = convert_text_to_float(text)
            elif 'Dereg Value' in label:
                data["dereg_value"] = convert_text_to_float(text)
            elif 'OMV' in label:
                data["omv"] = convert_text_to_float(text)
            elif 'ARF' in label:
                data["arf"] = convert_text_to_float(text)
            elif 'Engine Cap' in label:
                data["engine_capacity_cc"] = convert_text_to_float(text)
            elif 'Power' in label:
                cleaned_text = text.split(" ")[0]
                data["power"] = convert_text_to_float(cleaned_text)
            elif 'Curb Weight' in label:
                data["curb_weight"] = convert_text_to_float(text)
            elif 'No. of Owners' in label:
                data["no_of_owners"] = convert_text_to_float(text)
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

    return data




# def scrape_indiv_listing(listing_url, driver):
    driver.get(listing_url)
    content = driver.page_source
    html = BeautifulSoup(content, 'lxml')

    # Extract details from script
    details = html.find('script', type='application/ld+json')
    if details:
        details_json = json.loads(details.string)
        car_model = details_json.get('model', None) 
        brand = details_json.get('brand', {}).get('name', None)
        price_text = details_json.get('offers', {}).get('price', None)
        price = convert_text_to_float(price_text)
        color = details_json.get('color', None)
        fuel_type = details_json.get('vehicleEngine', {}).get('fuelType', None)
        manufactured_year = details_json.get('vehicleModelDate', None)
        transmission = details_json.get('vehicleTransmission', None)         
    else:
        car_model = html.find('a', class_='nounderline globaltitle').text.split("(")[0].strip()
        brand = car_model.split(" ")[0]
        price_text = html.find('tr', class_='row_bg').text
        price = convert_text_to_float(price_text)
        color = None
        fuel_type = None
        manufactured_year = None
        transmission = None
    
    price = float(re.sub(r'[^\d,\.]+', '', price_text).replace(',', ''))

    mileage = None
    depreciation         = None
    registration_date    = None
    coe_left             = None
    road_tax_per_year    = None
    dereg_value          = None
    omv                  = None
    coe_value            = None
    arf                  = None
    engine_capacity_cc   = None
    power                = None
    curb_weight          = None
    no_of_owners         = None
    vehicle_type         = None

    # Extract details from table
    table = html.find('table', id='carInfo')
    rows = table.find_all('tr')
    strongs_0 = rows[1].find_all('strong')
    for strong in strongs_0:
        text = strong.find_next('td').get_text(" ", strip=True)
        if 'Depreciation' in strong.text:
            if text != 'N.A.':
                depreciation = convert_text_to_float(text)
        if 'Reg Date' in strong.text:
            registration_date = parse_reg_date(text)
            coe_left = parse_coe_left(text)
    
    strongs_1 = rows[2].find_all('strong')
    for strong in strongs_1:
        print("strong: ", strong.text)
        text = strong.find_next('div', class_='row_info').get_text(" ", strip=True)
        print("text: ", text)
        if 'Mileage' in strong.text:
            text = text.split(" ")[0]
            mileage = convert_text_to_float(text)
        if 'Road Tax' in strong.text:
            road_tax_per_year = convert_text_to_float(text)
        if 'Dereg Value' in strong.text:
            dereg_value = convert_text_to_float
        if 'OMV' in strong.text:
            omv = convert_text_to_float(text)
        if 'ARF' in strong.text:
            arf = convert_text_to_float(text)
        if 'Engine Cap' in strong.text:
            engine_capacity_cc = convert_text_to_float(text)
        if 'Power' in strong.text:
            cleaned_text = text.split(" ")[0]
            power = float(cleaned_text.replace(',', ''))
        if 'Curb Weight' in strong.text:
            curb_weight = convert_text_to_float(text)
        if 'No. of Owners' in strong.text:
            no_of_owners = convert_text_to_float(text)
        if 'Type of Vehicle' in strong.text:
            vehicle_type = text
    
    if vehicle_type is None:
        vehicle_type = rows[3].find('a').get_text(" ", strip=True)
    
    coe = rows[2].find('a', href='popups/whatsCOE.php')
    coe_text = coe.find_next('div', class_='row_info').get_text(" ", strip=True)
    coe_value = convert_text_to_float(coe_text)
    
    row_data = {
        'listing_url': listing_url,
        'car_model': car_model,
        'brand': brand,
        'color': color,
        'fuel_type': fuel_type,
        'price': price,
        'depreciation': depreciation,
        'registration_date': registration_date,
        'coe_left': coe_left,
        'mileage': mileage,
        'manufactured_year': manufactured_year,
        'road_tax_per_year': road_tax_per_year,
        'transmission': transmission,
        'dereg_value': dereg_value,
        'omv': omv,
        'coe_value': coe_value,
        'arf': arf,
        'engine_capacity_cc': engine_capacity_cc,
        'power': power,
        'curb_weight': curb_weight,
        'no_of_owners': no_of_owners,
        'vehicle_type': vehicle_type,
    }

    return row_data

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
    
    # --- 1) Attempt JSON parse ---
    details_tag = html.find('script', type='application/ld+json')
    json_info = parse_json_details(details_tag, html)

    # --- 2) If JSON is missing, fallback to HTML parse ---
    if json_info["car_model"] is None:
        fallback_info = parse_html_fallback_details(html)
        for key, val in fallback_info.items():
            if json_info.get(key) is None:
                json_info[key] = val

    # --- 3) Convert price_text to float if not None ---
    price_text = json_info["price_text"]
    price = convert_text_to_float(price_text)

    # --- 4) Parse table details (Depreciation, RegDate, etc.) ---
    table_data = parse_table_details(html)

    # --- 5) Assemble final row_data ---
    row_data = {
        # 'listing_url': listing_url,
        'car_model': json_info["car_model"],
        'brand': json_info["brand"],
        'color': json_info["color"],
        'fuel_type': json_info["fuel_type"],
        'price': price,
        'depreciation': table_data["depreciation"],
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
    }

    return row_data


# Main Function
def main():
    url = 'https://www.sgcarmart.com/used-cars/listing?avl='

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
    print(date_string)
    date_string = date_string.replace('Posted ', '')
    lastest_date = datetime.datetime.strptime(date_string, "%d-%b-%Y").date()

    # Print
    print(f"=== Scrape Started ===")
    print(f"Date: {current_date}")
    print(f"Lastest Listing Date: {lastest_date}")
    print(f"Total Listings: {num_listings}")
    print(f"Total Pages: {num_pages}")
    print(f"======================")


    # =============== Start scraping through pages =============== 
    used_cars = []

    # Loop through all pages
    for page in range(1, 3):
        print(f"=== Page: {page}")
        
        if (page > 1):
            driver.get(url + f'&page={page}')
            content = driver.page_source
            html = BeautifulSoup(content, 'lxml')

        # Loop through all listings in a page 
        raw_el = html.find_all('a', class_='styles_text_link__wBaHL', href=True)
        listings = [el['href'] for el in raw_el[::2]] # to skip repeated links
        for i, listing in enumerate(listings, start=1):
            print(f"--- {i}: {listing}")
            car_data = scrape_indiv_listing(listing, driver)
            used_cars.append(car_data)

    df = pd.DataFrame(used_cars)
    df.to_csv('used_cars.csv', index=False)
    print("=== Scrape Completed ===")
    
    driver.quit()

if __name__ == "__main__":
    main()









