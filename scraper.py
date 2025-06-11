import configparser
import os
import time
import threading
import queue
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pyodbc 

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), 'config.ini')
CONFIG_DEVELOPMENT_FILE_PATH = os.path.join(os.path.dirname(__file__), 'config.development.ini')
JOBINDEX_URLS = {}
DATABASE_CONFIG = {}
JOBINDEX_BASE_URL = "https://www.jobindex.dk"

def setup_database_connection():
    if not os.path.exists(CONFIG_FILE_PATH):
        raise FileNotFoundError(f"Configuration file '{CONFIG_FILE_PATH}' not found. Please create it.")
    
    config = configparser.ConfigParser()

    if os.path.exists(CONFIG_DEVELOPMENT_FILE_PATH):
        config.read(CONFIG_DEVELOPMENT_FILE_PATH)
    else:
        config.read(CONFIG_FILE_PATH)

    if 'database' in config:
        DATABASE_CONFIG["server"] = config['database'].get("server")
        DATABASE_CONFIG["database"] = config['database'].get("database")
        DATABASE_CONFIG["username"] = config['database'].get("username")
        DATABASE_CONFIG["password"] = config['database'].get("password")
    else:
        print("Warning: [database] section not found in config.ini")

def setup_scraping_urls():
    for subid in range(1, 151):
        category_key = f"subid_{subid}"
        url = f"https://www.jobindex.dk/jobsoegning?subid={subid}"
        JOBINDEX_URLS[category_key] = url
    
    if not JOBINDEX_URLS:
        raise ValueError("No job index URLs found in configuration. Please check your config.ini.")

class DatabaseWriter(threading.Thread):
    def __init__(self, db_config, data_queue):
        super().__init__()
        self.db_config = db_config
        self.data_queue = data_queue
        self.running = True
        self.cnxn = None
        self.cursor = None

    def run(self):
        try:
            self.cnxn = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server};'
                f'SERVER={self.db_config["server"]};'
                f'DATABASE={self.db_config["database"]};'
                f'UID={self.db_config["username"]};'
                f'PWD={self.db_config["password"]}'
            )
            self.cursor = self.cnxn.cursor()
            print("[DB Writer] Successfully connected to MSSQL database.")
            setup_database(self.cursor)

            while self.running or not self.data_queue.empty():
                try:
                    job_data_list = self.data_queue.get(timeout=1) # Wait for data, or check every second
                    if job_data_list is None: # Sentinel value to stop thread
                        self.running = False
                        print("[DB Writer] Stop signal received.")
                        break

                    for job_data in job_data_list:
                        self.insert_job_data_single(job_data)
                    self.data_queue.task_done()
                except queue.Empty:
                    continue # No data, keep checking
                except Exception as e:
                    print(f"[DB Writer] Error processing item from queue: {e}")
                    self.data_queue.task_done() # Mark as done even on error to prevent blocking

        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"[DB Writer] Database connection error in writer thread: {sqlstate} - {ex}")
        finally:
            if self.cnxn:
                self.cnxn.close()
                print("[DB Writer] Database connection closed.")

    def insert_job_data_single(self, job_data):
        insert_query = """
        INSERT INTO JobIndexPostings (CompanyName, CompanyURL, JobTitle, JobLocation, JobDescription, JobUrl, Published, Category, BannerPicture, FooterPicture)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        try:
            self.cursor.execute(insert_query,
                                job_data["CompanyName"],
                                job_data["CompanyURL"],
                                job_data["JobTitle"],
                                job_data["JobLocation"],
                                job_data["JobDescription"],
                                job_data["JobUrl"],
                                job_data["Published"],
                                job_data["Category"],
                                job_data["BannerPicture"],
                                job_data["FooterPicture"])
            self.cnxn.commit()
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            if 'UNIQUE' in str(ex).upper():
                print(f"[DB Writer] Duplicate JobUrl, skipping: {job_data.get('JobUrl', 'N/A')}")
            else:
                print(f"[DB Writer] Error inserting data for {job_data.get('JobTitle', 'N/A')}: {sqlstate} - {ex}")
            self.cnxn.rollback()

def extract_job_data(html_content, category):
    """
    Extracts job data from a single HTML page.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    job_listings = []

    category_name = category
    category_span = soup.find('span', class_='filter-button__label')
    if category_span:
        category_name = category_span.get_text(strip=True)

    job_ad_wrappers = soup.find_all('div', id=lambda x: x and x.startswith('jobad-wrapper-'))

    for job_ad in job_ad_wrappers:
        company_name = None
        company_url = None
        job_title = None
        job_url = None
        job_location = None
        job_description = None
        published_date = None
        banner_picture_bytes = None
        footer_picture_bytes = None

        company_div = job_ad.find('div', class_='jix-toolbar-top__company')
        if company_div:
            company_link = company_div.find('a')
            if company_link:
                company_name = company_link.get_text(strip=True)
                company_url = JOBINDEX_BASE_URL + company_link.get('href')

        job_title_h4 = job_ad.find('h4')
        if job_title_h4:
            job_link = job_title_h4.find('a')
            if job_link:
                job_title = job_link.get_text(strip=True)
                job_url = job_link.get('href')

        job_location_div = job_ad.find('div', class_='jobad-element-area')
        if job_location_div:
            job_location_span = job_location_div.find('span')
            if job_location_span:
                job_location = job_location_span.get_text(strip=True)

        job_description_div = job_ad.find('div', class_='PaidJob-inner')
        if job_description_div:
            # Banner picture (first <center> with <img>)
            banner_center = job_description_div.find('center')
            if banner_center:
                banner_img = banner_center.find('img')
                if banner_img and banner_img.get('src'):
                    banner_url = banner_img['src']
                    if banner_url.startswith('/'):
                        banner_url = JOBINDEX_BASE_URL + banner_url
                    banner_picture_bytes = download_image_as_bytes(banner_url)

            # Footer picture (last <center> with <img>)
            centers = job_description_div.find_all('center')
            if centers:
                footer_center = centers[-1]
                footer_img = footer_center.find('img')
                if footer_img and footer_img.get('src'):
                    footer_url = footer_img['src']
                    if footer_url.startswith('/'):
                        footer_url = JOBINDEX_BASE_URL + footer_url
                    footer_picture_bytes = download_image_as_bytes(footer_url)

            description_parts = []
            for tag in job_description_div.find_all(['p', 'ul', 'li']):
                if tag.name == 'ul':
                    items = [li.get_text(strip=True) for li in tag.find_all('li')]
                    description_parts.append("\n".join(f"- {item}" for item in items))
                else:
                    description_parts.append(tag.get_text(strip=True))
            job_description = "\n".join(description_parts).strip()
            

        published_div = job_ad.find('div', class_='jix-toolbar__pubdate')
        if published_div:
            time_tag = published_div.find('time')
            if time_tag:
                published_date = time_tag.get('datetime')

        job_listings.append({
            "CompanyName": company_name,
            "CompanyURL": company_url,
            "JobTitle": job_title,
            "JobLocation": job_location,
            "JobDescription": job_description,
            "JobUrl": job_url,
            "Published": published_date,
            "Category": category_name,
            "BannerPicture": banner_picture_bytes,
            "FooterPicture": footer_picture_bytes,
        })
    return job_listings

def download_image_as_bytes(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.content
    except Exception as e:
        print(f"[Image Download] Failed to download {url}: {e}")
    return None

def setup_database(cursor):
    """
    Sets up the MSSQL database table if it doesn't exist.
    """
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='JobIndexPostings' and xtype='U')
    CREATE TABLE JobIndexPostings (
        JobID INT IDENTITY(1,1) PRIMARY KEY,
        CompanyName NVARCHAR(255),
        CompanyURL NVARCHAR(MAX),
        JobTitle NVARCHAR(MAX),
        JobLocation NVARCHAR(255),
        JobDescription NVARCHAR(MAX),
        JobUrl NVARCHAR(512) UNIQUE,
        Published DATETIME,
        Category NVARCHAR(255),
        BannerPicture VARBINARY(MAX),
        FooterPicture VARBINARY(MAX)
    )
    """
    try:
        cursor.execute(create_table_query)
        cursor.commit()
        print("[Main] Database table 'JobIndexPostings' checked/created successfully.")
    except Exception as e:
        print(f"[Main] Error setting up database table: {e}")

def scrape_and_store(start_url, db_config, category):
    """
    Main function to orchestrate scraping from a URL and storing, including pagination
    and asynchronous database writes.
    """
    driver = None
    data_queue = queue.Queue() # Queue for passing job data to the DB writer thread
    db_writer_thread = DatabaseWriter(db_config, data_queue)
    db_writer_thread.start()

    page_num = 1
    total_jobs_scraped = 0

    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(start_url)
        print(f"[Main] Navigating to {start_url} (Page {page_num})...")

        # --- Handle Cookie Consent ---
        try:
            print("[Main] Attempting to handle cookie consent...")
            cookie_accept_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "jix-cookie-consent-accept-all"))
            )
            cookie_accept_button.click()
            print("[Main] Cookie consent handled.")
            time.sleep(1)
        except Exception as e:
            print(f"[Main] No cookie consent button found (id='jix-cookie-consent-accept-all') or error handling: {e}. Proceeding anyway.")
        
        # --- Check for 404 Not Found Page (cookie consent will come first always) ---
        try:
            page_source = driver.page_source
            if '<h1>Siden kan ikke findes</h1>' in page_source:
                print(f"[Main] 404 Not Found detected for URL: {start_url}. Skipping this category.")
                return
        except Exception as e:
            print(f"[Main] Error checking for 404 page: {e}")

        # --- Handle JobAgent Modal ---
        try:
            print("[Main] Attempting to close jobagent modal...")
            jobagent_close_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.close[data-dismiss='modal'][aria-label='Luk']"))
            )
            jobagent_close_button.click()
            print("[Main] Jobagent modal closed.")
            time.sleep(1)
        except Exception as e:
            print(f"[Main] No jobagent modal close button found or error handling: {e}. Proceeding anyway.")

        print("[Main] Waiting for job listings to appear...")
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[id^='jobad-wrapper-']"))
        )
        print("[Main] Job listings found on page.")

        while True:
            html_content = driver.page_source
            current_page_listings = extract_job_data(html_content, category)
            
            if current_page_listings:
                data_queue.put(current_page_listings) 
                total_jobs_scraped += len(current_page_listings)
                print(f"[Main] Found {len(current_page_listings)} job listings on Page {page_num}. Added to queue.")
            else:
                print(f"[Main] No job listings found on Page {page_num}. This page might be empty or end of results.")


            # --- Pagination Logic ---
            try:
                pagination_ul = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "ul.pagination"))
                )

                next_page_li = pagination_ul.find_element(By.CSS_SELECTOR, "li.page-item-next")
                next_page_link = next_page_li.find_element(By.TAG_NAME, "a")
                next_page_url = next_page_link.get_attribute("href")

                if next_page_url:
                    page_num += 1
                    print(f"[Main] Navigating to next page: {next_page_url} (Page {page_num})...")
                    driver.get(next_page_url)
                    time.sleep(3)
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div[id^='jobad-wrapper-']"))
                    )
                else:
                    print("[Main] Next page URL not found. Ending pagination.")
                    break

            except Exception as e:
                print(f"[Main] No more next page or an error occurred during pagination: {e}")
                break

    except Exception as e:
        print(f"[Main] Selenium or general page loading error: {e}")
    finally:
        if driver:
            driver.quit()
            print("[Main] Selenium WebDriver closed.")

        # Signal the DB writer thread to stop and wait for it to finish
        print("[Main] Sending stop signal to DB writer and waiting for it to finish...")
        data_queue.put(None)
        db_writer_thread.join()
        print(f"[Main] Scraping completed. Total jobs added to queue: {total_jobs_scraped}")

if __name__ == "__main__":
    setup_database_connection()
    setup_scraping_urls()
    for category, url in JOBINDEX_URLS.items():
        print(f"[Main] Starting scrape for category '{category}' with URL: {url}")
        scrape_and_store(url, DATABASE_CONFIG, category)