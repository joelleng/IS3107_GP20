import re
import pandas as pd
import numpy as np

def string_extraction(value):
    try:
        num = int(value.replace(',', ''))
    except ValueError:
        num = np.nan  # Use np.nan if you're working with NumPy
    return num   

def get_car_model(json_data):
    """
    Car model data is not automatically displayed on the HTML of the website, 
    hence we have find this from the <script> tag as @context 
    This function utilises information from the <script> tag and in json format data.
    """
    car_name = json_data.get("model")
    return car_name

def get_car_model_html(url):
    """
    Car model data is obtainable from the HTML, this method retrieves car model from HTML
    """
    try:
        car_model = url.find('a', class_='nounderline globaltitle').text.split("(")[0].strip()
        return car_model
    except AttributeError:
        return None  # Return None if extraction fails

def get_car_brand(json_data):
    """
    Car Brand data is not automatically displayed on the HTML of the website, 
    hence we have find this from the <script> tag as @context 
    This function utilises information from the <script> tag and in json format data.
    """
    try:
        # Assuming 'json_data' has a 'brand' key with a 'name' key inside
        brand_name = json_data["brand"]["name"]
        return brand_name
    except KeyError:
        # In case of missing keys, return None (this will trigger the fallback)
        return None

def get_car_color(json_data):
    """
    Car color data is not automatically displayed on the HTML of the website, 
    hence we have find this from the <script> tag as @context 
    This function utilises information from the <script> tag and in json format data.
    """
    color = json_data.get("color")
    return color

def get_fuel_type(json_data):
    """
    Fuel type data is not automatically displayed on the HTML of the website, 
    hence we have find this from the <script> tag as @context 
    This function utilises information from the <script> tag and in json format data.
    """
    try:
        fuel_type = json_data.get("vehicleEngine")['fuelType']
        return fuel_type
    except KeyError:
        # In case of missing keys, return None (this will trigger the fallback)
        return None

def get_price(json_data):
    """
    Price data is obtainable from the HTML, however to avoid ValueError, IndexError and converting to int,
    We will use Price data from the <script> tag in @context 
    This function utilises information from the <script> tag and in json format data.
    """
    try:
        price = int(json_data["offers"]["price"])
        return price
    except KeyError:
        # In case of missing keys, return None (this will trigger the fallback)
        return None

def get_price_html(url):
    """
    Price data is obtainable from the HTML, this method retrieves price from HTML
    """
    # price = url.find(class_ = 'font_red').text.strip().split('$')
    price = url.find_all(class_='row_bg')[0].find_all('td')[1].text.strip().split(' ')[0].split('$')[1]
    price = string_extraction(price)
    return price

def get_depreciation(url):
    dep = url.find_all(class_='row_bg')[1].find_all('td')[1].text.strip().split(' /')[0].split('$')[1]
    dep = string_extraction(dep)
    return dep

def get_depreciation_soup(soup):
    depreciation_label = soup.find("td", class_="label", string=lambda text: text and "Depreciation" in text)

    # Find the next <td> element, which contains the depreciation value
    if depreciation_label:
        depreciation_value_td = depreciation_label.find_next("td")
        depreciation_text = depreciation_value_td.get_text(strip=True).split('/yr')[0]  # Extract value before "/yr"
        
        # Clean and convert to integer
        depreciation_value = int(depreciation_text.replace("$", "").replace(",", ""))
        
        return depreciation_value
    else:
        return np.nan

def get_reg_date(url):
    reg_date = url.find_all(class_='row_bg')[1].find_all('td')[3].text.strip().split('(')[0]
    return reg_date

def get_mileage(json_data):
    """
    Mileage data is obtainable from the HTML, however to avoid ValueError, IndexError and converting to int,
    We will use Mileage data from the <script> tag in @context 
    This function utilises information from the <script> tag and in json format data.
    """
    try:
        mileage = int(json_data["mileageFromOdometer"]["value"])
        return mileage if mileage != 0 else np.nan  # N.A. mileage is reflected as 0 in scripts, we cannot take 0 as the true mileage
    except (ValueError, KeyError, TypeError):
        return None

def get_mileage_html(url):
    """
    Mileage data is obtainable from the HTML, this method retrieves Mileage from HTML
    """
    try:
        miles = url.find_all(class_='row_info')[0].text.strip().split(' km')[0]
        miles = float(string_extraction(miles))
        return miles
    except (IndexError, ValueError):
        return np.nan

def get_manufactured_year(url):
    """
    Extracts the manufactured year from the given URL's HTML content.
    Handles cases where the required element does not exist.
    """
    try:
        date = url.find_all(class_='row_info')[6].text.strip()
        return int(date)
    except (IndexError, ValueError, AttributeError):
        return None  # Return None if extraction fails

def get_manufactured_year_soup(soup):
    try:
        rows = soup.find_all(class_='row_title')
        
        for row in rows:
            # Check if the row_title contains "Manufactured"
            if "Manufactured" in row.text:
                row_info = row.find_next_sibling(class_='row_info')
                if row_info:
                    manu_year = row_info.text.strip()
                    manu_year = int(manu_year)
                    return manu_year
    except (AttributeError, ValueError, TypeError):
        return None   # Return np.nan if "Manufactured" is not found

def helper_coe_clean(value):
    days_in_year = 365
    days_in_month = 30

    year_match = re.search(r'(\d+)\s*yrs?', value)
    month_match = re.search(r'(\d+)\s*mths?', value)
    day_match = re.search(r'(\d+)\s*days?', value)

    # Convert to integers (default to 0 if not found)
    years = int(year_match.group(1)) if year_match else 0
    months = int(month_match.group(1)) if month_match else 0
    days = int(day_match.group(1)) if day_match else 0

    return (years * days_in_year) + (months * days_in_month) + days

def get_coe_left(url):
    coe = url.find_all(class_='row_bg')[1].find_all('td')[3].text.strip().split('(')[1].split('COE')[0]
    coe = helper_coe_clean(coe)
    return coe
    
def get_road_tax(url):
    tax = url.find_all(class_='row_info')[1].text.strip().split(' ')[0].split('$')[1]
    tax = string_extraction(tax)
    return tax

def get_road_tax_soup(soup):
    try:
        rows = soup.find_all(class_='row_title')
        
        for row in rows:
            # Check if the row_title contains "Road Tax"
            if "Road Tax" in row.text:
                # Find the next sibling with class 'row_info'
                row_info = row.find_next_sibling(class_='row_info')
                if row_info:
                    tax_text = row_info.text.strip().split(' ')[0].replace('$', '')
                    return float(tax_text.replace(',', ''))  # Convert to float (handles decimals)
    except (AttributeError, ValueError, TypeError):
        return None  # Return np.nan if "Road Tax" is not found

def get_transmission(url):
    transmission = url.find_all(class_='row_info')[7].text.strip()
    return transmission

def get_transmission_soup(soup):
    try:
        rows = soup.find_all(class_='row_title')
        for row in rows:
            if "Transmission" in row.text:
                row_info = row.find_next_sibling(class_='row_info')
                if row_info:
                    return row_info.text.strip()
    except (AttributeError, ValueError, TypeError):
        return None # Return np.nan if "Transmission" is not found

def get_dereg_value(url):
    try:
        dereg_val = url.find_all(class_='row_info')[2].text.strip().split(' as')[0].split('$')[1]
        dereg_val = string_extraction(dereg_val)
    except:
        dereg_val = np.nan
    return dereg_val

def get_dereg_value_soup(soup):
    try:
        rows = soup.find_all(class_='row_title')
        for row in rows:
            if "Dereg Value" in row.text:
                row_info = row.find_next_sibling(class_='row_info')
                if row_info:
                    dereg = row_info.text.strip().split(' as')[0].split('$')[1]
                    dereg = float(string_extraction(dereg))
                    return dereg
    except (AttributeError, ValueError, TypeError):   
        return None  # Return np.nan if "Dereg Value" is not found

def get_omv(url):
    omv = url.find_all(class_='row_info')[8].text.strip().split('$')[1]
    omv = string_extraction(omv)
    return omv

def get_omv_soup(soup):
    try:
        rows = soup.find_all(class_='row_title')
        for row in rows:
            if "OMV" in row.text:
                # Find the next sibling with class 'row_info'
                row_info = row.find_next_sibling(class_='row_info')
                if row_info:
                    omv = row_info.text.strip().split('$')[1]
                    omv = float(string_extraction(omv))
                    return omv
    except (AttributeError, ValueError, TypeError):   
        return None  # Return np.nan if "OMV" is not found

def get_coe_value(url):
    try:
        coe_val = url.find_all(class_='row_info')[3].text.strip().split('$')[1]
        coe_val = string_extraction(coe_val)
    except:
        coe_val = np.nan
    return coe_val

def get_coe_value_soup(soup):
    try:
        rows = soup.find_all(class_='row_title')
        for row in rows:
            if "COE" in row.text:
                row_info = row.find_next_sibling(class_='row_info')
                if row_info:
                    coe = row_info.text.strip().split('$')[1]
                    coe = string_extraction(coe)
                    return coe
    except (AttributeError, ValueError, TypeError):   
        return None  # Return np.nan if "COE" is not found

def get_arf(url):
    arf_val = url.find_all(class_='row_info')[9].text.strip().split('$')[1]
    arf_val = string_extraction(arf_val)
    return arf_val

def get_arf_soup(soup):
    try:
        rows = soup.find_all(class_='row_title')
        for row in rows:
            if "ARF" in row.text:
                row_info = row.find_next_sibling(class_='row_info')
                if row_info:
                    arf = row_info.text.strip().split('$')[1]
                    arf = string_extraction(arf)
                return arf
    except (AttributeError, ValueError, TypeError):   
        return None  # Return np.nan if "ARF" is not found

def get_engine_capacity(url):
    engine_cap = url.find_all(class_='row_info')[4].text.strip().split(' cc')[0]
    engine_cap = string_extraction(engine_cap)
    return engine_cap

def get_power(url):
    try:
        power_text = url.find_all(class_='row_info')[10].text.strip().split(' kW')[0]
        power = float(power_text)
        return power
    except (IndexError, ValueError):
        return np.nan

def get_power_soup(soup):
    try:
        rows = soup.find_all(class_='row_title')
        for row in rows:
            if "Power" in row.text:
                row_info = row.find_next_sibling(class_='row_info')
                if row_info:
                    power_text = row_info.text.strip().split(' kW')[0]
                    try:
                        power = float(power_text)
                        return power
                    except ValueError:
                        return np.nan  # If conversion fails, return NaN
    except (AttributeError, ValueError, TypeError):   
        return None  # Return np.nan if "Power" is not found

def get_curb_weight(url):
    curb_weight = string_extraction(url.find_all(class_='row_info')[5].text.strip().split(' kg')[0])
    return curb_weight

def get_curb_weight_soup(soup):
    try:
        rows = soup.find_all(class_='row_title')
        for row in rows:
            if "Curb Weight" in row.text:
                row_info = row.find_next_sibling(class_='row_info')
                if row_info:
                    weight_text = row_info.text.strip().split(' kg')[0]
                    weight = string_extraction(weight_text)
                    return weight
    except (AttributeError, ValueError, TypeError):   
        return None  # Return np.nan if "Curb Weight" is not found

def get_number_of_owners(url):
    owner_text = url.find_all(class_='row_info')[11].text.strip()
    
    # Extract numbers using regex
    numbers = re.findall(r'\d+', owner_text)
    
    if not numbers:
        return np.nan 
    
    owner_num = int(numbers[0]) 

    # SGCarMart uses "less than" and "more than" to indicate ranges of owners
    if "less than" in owner_text.lower():
        owner_num -= 1  # Assume it's at most owner_num - 1
    elif "more than" in owner_text.lower():
        owner_num += 1  # Assume it's at least owner_num + 1

    return owner_num

def get_number_of_owners_soup(soup):
    try:
        rows = soup.find_all(class_='row_title')
        for row in rows:
            if "Owners" in row.text:
                row_info = row.find_next_sibling(class_='row_info')
                if row_info:
                    owner_text = row_info.text.strip()
                    numbers = re.findall(r'\d+', owner_text)
                    if not numbers:
                        return np.nan
                    owner_num = int(numbers[0])
                    if "less than" in owner_text.lower():
                        owner_num -= 1
                    elif "more than" in owner_text.lower():
                        owner_num += 1
                    return owner_num
    except (AttributeError, ValueError, TypeError):   
        return None  # Return np.nan if "Owners" is not found

def get_type_of_vehicle(json_data):
    try:
        types = json_data.get("bodyType")
        return types
    except KeyError:
        # In case of missing keys, return None (this will trigger the fallback)
        return None

def get_type_of_vehicle_html(url):
    types = url.find_all(class_='row_bg1')[0].find('a').text.strip()
    return types

def get_posted_date(soup):
    """Extracts the posted date from the HTML."""
    post_date_div = soup.find("div", id="usedcar_postdate")
    if post_date_div:
        parts = post_date_div.text.split("|")  # Split by the separator
        if len(parts) > 0:
            return parts[0].replace("Posted on:", "").strip()  # Extract posted date
    return None

def get_last_updated_date(soup):
    """Extracts only the date from the Last Updated text in the HTML."""
    post_date_div = soup.find("div", id="usedcar_postdate")
    if post_date_div:
        match = re.search(r"Last\s*Updated\s*on:\s*([\d\-A-Za-z]+)", post_date_div.text)
        if match:
            return match.group(1)  # Extracts only the date
    return None

def safe_extract(func, *args, default=np.nan):
    """Utility function to safely extract values with a fallback."""
    try:
        return func(*args)
    except:
        return default