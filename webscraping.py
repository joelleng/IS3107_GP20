import re
import json
import datetime
import pandas as pd
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import time

# Configuration for new cars
class ScraperConfig:
    def __init__(self):
        self.base_url = 'https://www.sgcarmart.com/new-cars/listing/search'
        self.overview_url_template = 'https://www.sgcarmart.com/new_cars/newcars_overview.php?CarCode={car_code}'
        self.specs_url_template = 'https://www.sgcarmart.com/new_cars/newcars_specs.php?CarCode={car_code}'
        self.listing_selector = 'div.styles_textModelName__BauxN a[href*="CarCode"]'
        self.json_selector = 'script[type="application/ld+json"]'
        
# Utility Functions
def convert_text_to_float(text: str) -> Optional[float]:
    if not text or text in ['N.A.', 'unknown']:
        return None
    cleaned_text = re.sub(r'[^\d,\.]+', '', text)
    return float(cleaned_text.replace(',', ''))

def parse_dimensions(text: str) -> Dict[str, Optional[float]]:
    match = re.search(r'\((\d+)\s*[x×]\s*(\d+)\s*[x×]\s*(\d+)\)\s*mm', text)
    return {
        'length_mm': float(match.group(1)) if match else None,
        'width_mm': float(match.group(2)) if match else None,
        'height_mm': float(match.group(3)) if match else None
    }

def parse_price_range(text: str) -> Dict[str, Optional[float]]:
    if not text or '-' not in text:
        return {'min_price': convert_text_to_float(text), 'max_price': None}
    min_price, max_price = text.split(' - ')
    return {
        'min_price': convert_text_to_float(min_price),
        'max_price': convert_text_to_float(max_price)
    }



# Data Parser Class
class NewCarDataParser:
    @staticmethod
    def parse_overview_html(html: BeautifulSoup) -> Dict[str, Optional[any]]:
        data = {'car_model': None, 'price': None, 'brand': None}
        
        # Extract brand and car model from navigation div
        nav_div = html.find('div', id='text_navigation')
        if nav_div:
            # Find the last two links in the navigation
            brand_links = nav_div.find_all('a')
            if len(brand_links) >= 2:
                # Second to last link is the brand
                data['brand'] = brand_links[-2].text.strip()
                # Last link is the full car model
                data['car_model'] = brand_links[-1].text.strip()
        
        # Find price in specific nested structure
        def extract_price(elem):
            # Find span with font_bold class within the element
            price_span = elem.find('span', class_='font_bold')
            
            if price_span:
                # Extract price text
                price_text = price_span.get_text(strip=True)
                
                # Use regex to find dollar value with commas
                price_match = re.search(r'\$\s*[\d,]+', price_text)
                if price_match:
                    return convert_text_to_float(price_match.group(0))
            
            return None

        # Search for td elements with specific structure
        price_candidates = html.find_all('td', class_='font_red')
        
        # Iterate through candidates to find the first valid price
        for candidate in price_candidates:
            price = extract_price(candidate)
            if price is not None:
                data['price'] = price
                break
        
        return data

    @staticmethod
    def parse_specs_html(html: BeautifulSoup) -> Dict[str, Optional[any]]:
        data = {
            'fuel_type': None, 'engine_type': None, 'battery_type': None, 'drive_type': None,
            'transmission': None, 'power_kw': None, 'torque_nm': None, 'top_speed_kmh': None,
            'vehicle_type': None, 'seating_capacity': None, 'boot_capacity_litres': None, 
            'battery_capacity_kwh': None, 'drive_range_km': None,'energy_consumption_km_per_kwh': None, 
            'ac_charging_rate_kw': None, 'ac_charging_time_h': None,
            'dc_charging_rate_kw': None, 'dc_charging_time_h': None,
            'fuel_tank_capacity_litres' : None , 'mileage' : None
        }
        
        spec_table = html.find('table',id = 'submodel_spec')
        if not spec_table:
            return data
        
        rows = spec_table.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) != 2:
                continue
            label = cells[0].text.strip()
            value = cells[1].text.strip()
            
            if label == 'Engine Type':
                data['engine_type'] = value
            elif label == 'Battery Type':
                data['battery_type'] = value
            elif label == 'Fuel Type':
                data['fuel_type'] = value
            elif label == 'Drive Type':
                data['drive_type'] = value
            elif label == 'Transmission':
                data['transmission'] = value
            elif label == 'Engine Power':
                power_match = re.search(r'(\d+)\s*kW', value)
                data['power_kw'] = float(power_match.group(1)) if power_match else convert_text_to_float(value)
            elif label == 'Power':
                power_match = re.search(r'(\d+)\s*kW', value)
                data['power_kw'] = float(power_match.group(1)) if power_match else convert_text_to_float(value)
            elif label == 'Torque':
                data['torque_nm'] = convert_text_to_float(value)
            elif label == 'Top Speed':
                data['top_speed_kmh'] = convert_text_to_float(value)
            elif label == 'Boot/Cargo Capacity':
                data['boot_capacity_litres'] = convert_text_to_float(value)
            elif label == 'Vehicle Type':
                data['vehicle_type'] = value
            elif label == 'Seating Capacity':
                data['seating_capacity'] = int(value) if value.isdigit() else None
            
            # For ICE engines
            elif label == 'Fuel Tank Capacity':
                data['fuel_tank_capacity_litres'] = convert_text_to_float(value)
            elif label == 'Fuel Consumption':
                data['mileage'] = convert_text_to_float(value)
            
            # For electric vehicles
            elif label == 'Battery Capacity':
                data['battery_capacity_kwh'] = convert_text_to_float(value)
            elif label == 'Drive Range':
                data['drive_range_km'] = convert_text_to_float(value)
            elif label == 'Energy Consumption':
                data['energy_consumption_km_per_kwh'] = convert_text_to_float(value.split()[0])
            elif label == 'AC Max Charging Rate':
                data['ac_charging_rate_kw'] = convert_text_to_float(value)
            elif label == 'AC Charging Time':
                data['ac_charging_time_h'] = convert_text_to_float(value)
            elif label == 'DC Max Charging Rate':
                data['dc_charging_rate_kw'] = convert_text_to_float(value)
            elif label == 'DC Charging Time':
                data['dc_charging_time_h'] = convert_text_to_float(value)
    
        return data
    
# Scraper Class
class NewCarScraper:
    def __init__(self):
        self.config = ScraperConfig()
        self.driver = self._setup_driver()
        self.parser = NewCarDataParser()
        self.scraped_car_codes = set()  # Track unique car codes

    def _setup_driver(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--window-size=2000,1080")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
        return webdriver.Chrome(options=options)

    def get_page_content(self, url: str) -> BeautifulSoup:
        try:
            self.driver.get(url)
            return BeautifulSoup(self.driver.page_source, 'lxml')
        except:
            print("Page Unavailable")
            return None

    def scrape_listing(self, car_code: str) -> Dict[str, any]:
        overview_url = self.config.overview_url_template.format(car_code=car_code)
        overview_html = self.get_page_content(overview_url)
        if overview_html:
            overview_data = self.parser.parse_overview_html(overview_html)

        specs_url = self.config.specs_url_template.format(car_code=car_code)
        specs_html = self.get_page_content(specs_url)
        # specs_json = self.parser.parse_specs_html(specs_html.find(self.config.json_selector))
        if specs_html:
            specs_data = self.parser.parse_specs_html(specs_html)

        return {
            'listing_url': overview_url,
            'car_code': car_code,
            **overview_data,
            # **specs_json,
            **specs_data
        }

    def scrape_summary(self, html: BeautifulSoup) -> Dict[str, any]:
        pages_div = html.find_all('div', class_='SGCM_UIKIT_REACT_Select_textActiveOption_do0ax_75') 
        num_pages = 1   
        for containers in pages_div:
            page_match = re.search(r'of (\d+)', containers.text)
            if page_match:
                # Explicitly convert the matched group to integer
                print(page_match.group(1))
                num_pages = int(page_match.group(1))
                print(f"Total pages: {num_pages}")  # Debugging print
                break  # Stop after finding the first match

        count_containers = html.find_all('div', class_='div.styles_containerMobileCard__4F_wh')
        num_listings = len(count_containers)*num_pages
        
        return {
            "num_listings": num_listings,
            "num_pages": num_pages,
            "current_date": datetime.date.today()
        }

    def scrape_all(self,start, max_pages: Optional[int] = None) -> List[Dict[str, any]]:
        results = []
        self.scraped_car_codes.clear()  # Reset for each run
        html = self.get_page_content(self.config.base_url)
        summary = self.scrape_summary(html)
        
        print(f"=== Scrape Started ===\n"
              f"Date: {summary['current_date']}\n"
              f"Total Listings: {summary['num_listings']}\n"
              f"Total Pages: {summary['num_pages']}\n"
              f"======================")

        pages_to_scrape = min(max_pages or summary["num_pages"], summary["num_pages"])
        for page in range(start, pages_to_scrape + 1):
            print(f"=== Page: {page}")
            if page > 1:
                html = self.get_page_content(f"{self.config.base_url}?page={page}")
                if not html:
                    continue
            
            listings = html.find_all('a', href=re.compile(r'CarCode=\d+'))
            for i, listing in enumerate(listings, 1):
                href = listing['href']
                car_code_match = re.search(r'CarCode=(\d+)', href)
                if car_code_match:
                    car_code = car_code_match.group(1)
                    if car_code not in self.scraped_car_codes:  # Check for uniqueness
                        self.scraped_car_codes.add(car_code)
                        print(f"--- {i}: {href}")
                        results.append(self.scrape_listing(car_code))
                    else:
                        print(f"--- {i}: {href} (Skipped - Already scraped CarCode {car_code})")

        self.driver.quit()
        return results

def main():
    scraper = NewCarScraper()
    # Read existing cars
    try:
        cars = pd.read_csv('new_cars.csv')
    except FileNotFoundError:
        print("No existing CSV found. Creating a new one.")
        cars = pd.DataFrame()
    new_cars = scraper.scrape_all(start = 3,max_pages=50)
    new_cars_df = pd.DataFrame(new_cars)
    new_cars_df = pd.concat([cars,new_cars_df]).drop_duplicates()
    new_cars_df.to_csv('new_cars.csv', index=False)
    print("=== Scrape Completed ===")

if __name__ == "__main__":
    main()