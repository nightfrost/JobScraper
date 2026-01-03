import configparser
import os
import time
import threading
import queue
import requests
import tempfile
import re
from datetime import datetime, timezone
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pyodbc 
import spacy
from spacy.util import is_package

try:
    import yake  # Optional keyword extractor
except ImportError:
    yake = None

CONFIG_FILE_PATH = os.path.join(os.path.dirname(__file__), 'config.ini')
CONFIG_DEVELOPMENT_FILE_PATH = os.path.join(os.path.dirname(__file__), 'config.development.ini')
JOBINDEX_URLS = {}
DATABASE_CONFIG = {}
JOBINDEX_BASE_URL = "https://www.jobindex.dk"
EXISTING_JOB_URLS = set()
"""
Global banned keywords list. These are filtered from JobKeywords. Combined with spaCy DA/EN stopwords and optional config overrides.
"""
BANNED_KEYWORDS = set()
_DK_COMMON = [
    # Danish common function words / pronouns / determiners / prepositions
    "og", "i", "på", "af", "for", "med", "til", "fra", "om", "over", "under",
    "den", "det", "de", "der", "som", "en", "et", "din", "dit", "dine", "min", "mit", "mine",
    "han", "hun", "vi", "jeg", "du", "man", "jer", "os",
    "er", "var", "bliver", "blev", "kan", "skal", "må", "bør", "kun", "ikke",
]
_EN_COMMON = [
    # English common stopwords (short subset)
    "the", "and", "or", "for", "to", "of", "in", "on", "at", "by", "from", "is", "are", "was", "were",
    "this", "that", "these", "those", "your", "our", "their", "his", "her", "its", "you", "we", "they",
]
BANNED_KEYWORDS.update({w.lower() for w in _DK_COMMON})
BANNED_KEYWORDS.update({w.lower() for w in _EN_COMMON})
EXTRA_BANNED_KEYWORDS = set()

TARGET_KEYWORD_COUNT = 20

YAKE_EXTRACTOR_DA = None
YAKE_EXTRACTOR_EN = None
if yake:
    try:
        YAKE_EXTRACTOR_DA = yake.KeywordExtractor(lan="da", n=1, top=TARGET_KEYWORD_COUNT)
    except Exception as e:
        print(f"[Keywords] Failed to initialize YAKE Danish extractor: {e}")
        YAKE_EXTRACTOR_DA = None
    try:
        YAKE_EXTRACTOR_EN = yake.KeywordExtractor(lan="en", n=1, top=TARGET_KEYWORD_COUNT)
    except Exception as e:
        print(f"[Keywords] Failed to initialize YAKE English extractor: {e}")
        YAKE_EXTRACTOR_EN = None

def setup_existing_joburls():
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={DATABASE_CONFIG["server"]};'
        f'DATABASE={DATABASE_CONFIG["database"]};'
        f'UID={DATABASE_CONFIG["username"]};'
        f'PWD={DATABASE_CONFIG["password"]}'
    )
    cursor = conn.cursor()
    cursor.execute("SELECT JobUrl FROM JobIndexPostingsExtended")
    rows = cursor.fetchall()
    for row in rows:
        EXISTING_JOB_URLS.add(row[0])
    cursor.close()
    conn.close()

def setup_database_connection():
    if not os.path.exists(CONFIG_FILE_PATH):
        raise FileNotFoundError(f"Configuration file '{CONFIG_FILE_PATH}' not found. Please create it.")
    
    config = configparser.ConfigParser()

    # Read default first, then overlay development if present (dev overrides defaults)
    if os.path.exists(CONFIG_DEVELOPMENT_FILE_PATH):
        config.read([CONFIG_FILE_PATH, CONFIG_DEVELOPMENT_FILE_PATH])
    else:
        config.read([CONFIG_FILE_PATH])

    if 'database' in config:
        DATABASE_CONFIG["server"] = config['database'].get("server")
        DATABASE_CONFIG["database"] = config['database'].get("database")
        DATABASE_CONFIG["username"] = config['database'].get("username")
        DATABASE_CONFIG["password"] = config['database'].get("password")
    else:
        print("Warning: [database] section not found in config.ini")
    # Optional: read extra banned keywords from config
    if 'keywords' in config:
        def load_banned(raw_value):
            if not raw_value:
                return
            parts_local = re.split(r'[\n,;]+', raw_value)
            EXTRA_BANNED_KEYWORDS.update({p.strip().lower() for p in parts_local if p.strip()})

        load_banned(config['keywords'].get('banned', fallback=''))
        load_banned(config['keywords'].get('banned_danish', fallback=''))
        load_banned(config['keywords'].get('banned_english', fallback=''))

def setup_scraping_urls():
    for subid in range(1, 250):
        category_key = f"subid_{subid}"
        url = f"https://www.jobindex.dk/jobsoegning?subid={subid}"
        JOBINDEX_URLS[category_key] = url
    
    if not JOBINDEX_URLS:
        raise ValueError("No job index URLs found in configuration. Please check your config.ini.")

class DatabaseWriter(threading.Thread):
    def update_category_for_joburl(self, job_url, new_category):
        """
        Link the job to the new category in the join table (normalized schema).
        """
        try:
            category_id = get_or_create_category(self.cursor, new_category)
            job_id = get_jobid_by_url(self.cursor, job_url)
            if job_id and category_id:
                link_job_category(self.cursor, job_id, category_id)
                self.cnxn.commit()
                print(f"[DB Writer] Linked job {job_url} to category '{new_category}' (ID {category_id})")
        except Exception as e:
            print(f"[DB Writer] Error updating category for {job_url}: {e}")

    def update_seen_last_for_joburl(self, job_url):
        """Update the SeenLast marker for an existing job and mark active."""
        try:
            self.cursor.execute(
                "UPDATE JobIndexPostingsExtended SET SeenLast = ?, IsActive = 1 WHERE JobUrl = ?",
                datetime.now(timezone.utc),
                job_url
            )
        except pyodbc.Error as ex:
            print(f"[DB Writer] Error updating SeenLast for {job_url}: {ex}")

    def __init__(self, db_config, data_queue, batch_size=20):
        super().__init__()
        self.db_config = db_config
        self.data_queue = data_queue
        self.running = True
        self.cnxn = None
        self.cursor = None
        self.batch = []
        self.batch_size = batch_size

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
                    job_data_list = self.data_queue.get(timeout=1)
                    if job_data_list is None:
                        self.running = False
                        print("[DB Writer] Stop signal received.")
                        # Flush remaining batch before exit
                        if self.batch:
                            self.insert_job_data_batch(self.batch)
                            self.batch = []
                        break

                    for job_data in job_data_list:
                        self.batch.append(job_data)
                        if len(self.batch) >= self.batch_size:
                            self.insert_job_data_batch(self.batch)
                            self.batch = []
                    self.data_queue.task_done()
                except queue.Empty:
                    continue
                except Exception as e:
                    print(f"[DB Writer] Error processing item from queue: {e}")
                    self.data_queue.task_done()

            # Final flush if any jobs left
            if self.batch:
                self.insert_job_data_batch(self.batch)
                self.batch = []

        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"[DB Writer] Database connection error in writer thread: {sqlstate} - {ex}")
        finally:
            if self.cnxn:
                self.cnxn.close()
                print("[DB Writer] Database connection closed.")

    def insert_job_data_batch(self, job_batch):
        try:
            for job_data in job_batch:
                self.cursor.execute("SELECT JobUrl FROM JobIndexPostingsExtended WHERE JobUrl = ?", job_data["JobUrl"])
                if self.cursor.fetchone():
                    self.update_seen_last_for_joburl(job_data["JobUrl"])
                    self.update_category_for_joburl(job_data["JobUrl"], job_data["Category"])
                else:
                    self.insert_job_data_single(job_data, commit=False)
            self.cnxn.commit()
        except pyodbc.Error as ex:
            print(f"[DB Writer] Batch insert error: {ex}")
            self.cnxn.rollback()

    def insert_job_data_single(self, job_data, commit=True):
        # Note: We no longer store keywords in JobIndexPostingsExtended. They go into JobKeywords.
        insert_query = """
        INSERT INTO JobIndexPostingsExtended (CompanyName, CompanyURL, JobTitle, JobLocation, JobDescription, JobUrl, Published, BannerPicture, FooterPicture, SeenLast, IsActive)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                                job_data["BannerPicture"],
                                job_data["FooterPicture"],
                                datetime.now(timezone.utc),
                                1)
            self.cursor.execute("SELECT JobID FROM JobIndexPostingsExtended WHERE JobUrl = ?", job_data["JobUrl"])
            row = self.cursor.fetchone()
            if row:
                job_id = row[0]
                category_id = get_or_create_category(self.cursor, job_data["Category"])
                if category_id:
                    link_job_category(self.cursor, job_id, category_id)
                # Insert keywords into the new JobKeywords table
                if job_data.get("KeywordsDetailed"):
                    insert_keywords(self.cursor, job_id, job_data["KeywordsDetailed"])
            if commit:
                self.cnxn.commit()
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            if 'UNIQUE' in str(ex).upper():
                print(f"[DB Writer] Duplicate JobUrl, skipping: {job_data.get('JobUrl', 'N/A')}")
            else:
                print(f"[DB Writer] Error inserting data for {job_data.get('JobTitle', 'N/A')}: {sqlstate} - {ex}")
            if commit:
                self.cnxn.rollback()

    # ...existing code...

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


    def get_main_content(soup2, job_url_full):
        # Returns main_content, text_blocks, full_text
        def extract_text_blocks(main_content):
            text_blocks = []
            if main_content:
                for tag in main_content.find_all(['h1', 'h2', 'h3', 'p', 'li', 'span']):
                    txt = tag.get_text(separator=" ", strip=True)
                    if txt and len(txt) > 30:
                        text_blocks.append(txt)
            return text_blocks

        # Custom handling for known sites
        def try_generic_if_empty(main_content, text_blocks, full_text):
            # If main_content is None or text_blocks/full_text is empty, try generic extraction
            if (main_content is None or not text_blocks or not full_text.strip()):
                # --- Begin generic extraction (copied from below) ---
                import re
                main_content = None
                class_regex = re.compile(r'(jobtext-jobad__main|job-text|jobtext|jobcontent|job|js-primary-contents-container|job-posting-details|AdContentContainer)', re.I)
                main_content = soup2.find('div', class_=class_regex)
                if not main_content:
                    main_content = soup2.find('div', attrs={"data-automation-id": re.compile(r'job[-_]posting[-_]details', re.I)})
                if not main_content:
                    main_content = soup2.find('div', attrs={"class": re.compile(r'job|posting', re.I)})
                text_blocks = extract_text_blocks(main_content)
                if not text_blocks:
                    for tag in soup2.find_all(['h1', 'h2', 'h3', 'p', 'li', 'span']):
                        txt = tag.get_text(separator=" ", strip=True)
                        if txt and len(txt) > 30:
                            text_blocks.append(txt)
                full_text = "\n".join(text_blocks)
                # Remove unwanted boilerplate text for jobindex.dk
                if "jobindex.dk" in job_url_full:
                    unwanted_phrases = [
                        "Søg jobbet nemt fra mobilen Jobannoncearkiv Find inspiration i udløbne jobopslag Få en jobvejleder Bliv afklaret i din jobsøgning Jobs For Ukraine Hjælper ukrainske flygtninge med at få job",
                        "Søg job Vælg mellem flere søgekriterier",
                        "Din side Skab overblik over din jobsøgning",
                        "Jobagent",
                        "Få en jobvejleder Bliv afklaret i din jobsøgning",
                        "Jobs For Ukraine Hjælper ukrainske flygtninge med at få job",
                        "Arbejdspladser Se virksomhedsprofiler Få viden om din næste arbejdsplads Evaluér arbejdsplads"
                    ]
                    for phrase in unwanted_phrases:
                        full_text = full_text.replace(phrase, "")
            return main_content, text_blocks, full_text

        if "systematic.com" in job_url_full:
            main_content = soup2.find('div', class_='job')
            text_blocks = extract_text_blocks(main_content)
            full_text = "\n".join(text_blocks) if text_blocks else soup2.get_text(separator=" ", strip=True)
            return try_generic_if_empty(main_content, text_blocks, full_text)
        elif "www.jobindex.dk/jobannonce/" in job_url_full:
            for cls in ['jobtext-jobad__main', 'job-text', 'jobtext', 'jobcontent', 'job']:
                main_content = soup2.find('div', class_=cls)
                if main_content:
                    break
            text_blocks = extract_text_blocks(main_content)
            full_text = "\n".join(text_blocks) if text_blocks else soup2.get_text(separator=" ", strip=True)
            return try_generic_if_empty(main_content, text_blocks, full_text)
        elif "candidate.hr-manager.net" in job_url_full:
            main_content = soup2.find('div', class_='AdContentContainer')
            text_blocks = extract_text_blocks(main_content)
            full_text = "\n".join(text_blocks) if text_blocks else soup2.get_text(separator=" ", strip=True)
            return try_generic_if_empty(main_content, text_blocks, full_text)
        elif "myworkdayjobs.com" in job_url_full:
            main_content = soup2.find('div', attrs={'data-automation-id': 'job-posting-details', 'class': 'css-11p01j8'})
            text_blocks = extract_text_blocks(main_content)
            full_text = "\n".join(text_blocks) if text_blocks else soup2.get_text(separator=" ", strip=True)
            return try_generic_if_empty(main_content, text_blocks, full_text)
        elif "jyskebank.dk" in job_url_full:
            main_content = soup2.find('div', class_='umb-richtext')
            text_blocks = extract_text_blocks(main_content)
            full_text = "\n".join(text_blocks) if text_blocks else soup2.get_text(separator=" ", strip=True)
            return try_generic_if_empty(main_content, text_blocks, full_text)
        elif "kirklarsen.dk" in job_url_full:
            main_content = soup2.find('div', class_='js-primary-contents-container')
            text_blocks = extract_text_blocks(main_content)
            full_text = "\n".join(text_blocks) if text_blocks else soup2.get_text(separator=" ", strip=True)
            return try_generic_if_empty(main_content, text_blocks, full_text)
        elif "jobbank.dk" in job_url_full:
            main_content = soup2.find('div', class_='jobText')
            text_blocks = extract_text_blocks(main_content)
            full_text = "\n".join(text_blocks) if text_blocks else soup2.get_text(separator=" ", strip=True)
            return try_generic_if_empty(main_content, text_blocks, full_text)
        elif "oraclecloud.com" in job_url_full:
            main_content = soup2.find('div', class_='job-details__description-content')
            text_blocks = extract_text_blocks(main_content)
            full_text = "\n".join(text_blocks) if text_blocks else soup2.get_text(separator=" ", strip=True)
            return try_generic_if_empty(main_content, text_blocks, full_text)
        return try_generic_if_empty(main_content=None, text_blocks=[], full_text="")

    def extract_job_description(full_text, category_name):
        doc_da = nlp_da(full_text)
        doc_en = nlp_en(full_text)
        def extract_relevant_sentences(doc, category_name):
            sents = list(doc.sents)
            scored = []
            for s in sents:
                score = 0
                if category_name and category_name.lower() in s.text.lower():
                    score += 1
                score += len(s.ents)
                score += len(s.text) // 100  # less strict: longer sentences get a bit more weight
                if len(s.text.strip()) >= 20:  # less strict: allow shorter sentences
                    scored.append((score, s.text))
            scored.sort(reverse=True)
            top_sentences = [t for _, t in scored[:20]]  # less strict: more sentences
            if sum(len(s) for s in top_sentences) < 200 and len(scored) > 20:
                for _, t in scored[20:]:
                    top_sentences.append(t)
                    if sum(len(s) for s in top_sentences) >= 200:
                        break
            return top_sentences
        relevant = extract_relevant_sentences(doc_da, category_name)
        if not relevant or sum(len(s) for s in relevant) < 60:
            relevant = extract_relevant_sentences(doc_en, category_name)
        if relevant and sum(len(s) for s in relevant) >= 60:
            return "\n".join(relevant).strip()
        else:
            # fallback: less strict, return more of the original text
            return full_text[:1500]

    def dedupe_description_lines(text):
        if not text:
            return text
        seen = set()
        deduped_lines = []
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped in seen:
                continue
            seen.add(stripped)
            deduped_lines.append(stripped)
        return "\n".join(deduped_lines) if deduped_lines else None

    def extract_fallbacks(company_name, company_url, job_location, doc_da, doc_en, full_text):
        def is_missing(val):
            return val is None or (isinstance(val, str) and not val.strip())
        # Fallback CompanyName: first ORG entity (DA, then EN)
        if is_missing(company_name):
            orgs = [ent.text for ent in doc_da.ents if ent.label_ == "ORG"]
            if not orgs:
                orgs = [ent.text for ent in doc_en.ents if ent.label_ == "ORG"]
            if orgs:
                company_name = orgs[0]
        # Fallback CompanyURL: look for a URL in the text (simple regex)
        if is_missing(company_url):
            import re
            url_pattern = r"https?://[\w\.-]+(?:/[\w\.-]*)*"
            urls_found = re.findall(url_pattern, full_text)
            urls_found = [u for u in urls_found if not any(x in u for x in ["jobindex.dk", "systematic.dk"])]
            if urls_found:
                company_url = urls_found[0]
        # Fallback JobLocation: first GPE or LOC entity (DA, then EN)
        if is_missing(job_location):
            locs = [ent.text for ent in doc_da.ents if ent.label_ in ("GPE", "LOC")]
            if not locs:
                locs = [ent.text for ent in doc_en.ents if ent.label_ in ("GPE", "LOC")]
            if locs:
                job_location = locs[0]
        return company_name, company_url, job_location

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

        job_title_h4 = job_ad.find('h4')
        if job_title_h4:
            job_link = job_title_h4.find('a')
            if job_link:
                job_title = job_link.get_text(strip=True)
                job_url = job_link.get('href')

        if not job_url:
            print(f"[JobUrl Skipped] JobUrl is invalid: {job_url}")
            continue

        company_div = job_ad.find('div', class_='jix-toolbar-top__company')
        if company_div:
            company_link = company_div.find('a')
            if company_link:
                company_name = company_link.get_text(strip=True)
                company_url = JOBINDEX_BASE_URL + company_link.get('href')

        job_location_div = job_ad.find('div', class_='jobad-element-area')
        if job_location_div:
            job_location_span = job_location_div.find('span')
            if job_location_span:
                job_location = job_location_span.get_text(strip=True)

        job_description_div = job_ad.find('div', class_='PaidJob-inner')
        if job_description_div:
            banner_center = job_description_div.find('center')
            if banner_center:
                banner_img = banner_center.find('img')
                if banner_img and banner_img.get('src'):
                    banner_url = banner_img['src']
                    if banner_url.startswith('/'):
                        banner_url = JOBINDEX_BASE_URL + banner_url
                    banner_picture_bytes = download_image_as_bytes(banner_url)
            centers = job_description_div.find_all('center')
            if centers:
                footer_center = centers[-1]
                footer_img = footer_center.find('img')
                if footer_img and footer_img.get('src'):
                    footer_url = footer_img['src']
                    if footer_url.startswith('/'):
                        footer_url = JOBINDEX_BASE_URL + footer_url
                    footer_picture_bytes = download_image_as_bytes(footer_url)

        if job_url:
            if job_url in EXISTING_JOB_URLS:
                print(f"[JobUrl Skipped] JobUrl already exists, skipping SpaCy extraction.: {job_url}")
            else:
                try:
                    if job_url.startswith('/'):
                        job_url_full = JOBINDEX_BASE_URL + job_url
                    elif job_url.startswith('http'):
                        job_url_full = job_url
                    else:
                        job_url_full = JOBINDEX_BASE_URL + '/' + job_url.lstrip('/')
                    resp = requests.get(job_url_full, timeout=5)
                    if resp.status_code == 200:
                        soup2 = BeautifulSoup(resp.text, 'html.parser')
                        _, _, full_text = get_main_content(soup2, job_url_full)
                        job_description = extract_job_description(full_text, category_name)
                        job_description = dedupe_description_lines(job_description)
                        doc_da = nlp_da(full_text)
                        doc_en = nlp_en(full_text)
                        company_name, company_url, job_location = extract_fallbacks(company_name, company_url, job_location, doc_da, doc_en, full_text)
                except Exception as e:
                    print(f"[JobUrl Description] Failed to fetch or extract description from {job_url}: {e}")

        published_div = job_ad.find('div', class_='jix-toolbar__pubdate')
        if published_div:
            time_tag = published_div.find('time')
            if time_tag:
                published_date = time_tag.get('datetime')

        # --- Keyword extraction using YAKE (Danish and English) and category keyword ---
        keywords_detailed = []  # list of dicts: {Keyword, Source, ConfidenceScore}
        keywords_for_display = []
        try:
            stop_category = category_name.lower() if isinstance(category_name, str) else None
        except Exception:
            stop_category = None
        custom_stopwords = {"job", "stilling", "company", "virksomhed", "arbejde", "position", "ansøgning", "opgaver"}
        # Merge banned keywords and spaCy DA/EN stopwords
        try:
            custom_stopwords.update({w.lower() for w in nlp_da.Defaults.stop_words})
        except Exception:
            pass
        try:
            custom_stopwords.update({w.lower() for w in nlp_en.Defaults.stop_words})
        except Exception:
            pass
        custom_stopwords.update(BANNED_KEYWORDS)
        custom_stopwords.update(EXTRA_BANNED_KEYWORDS)
        if stop_category:
            custom_stopwords.add(stop_category)
        def add_keyword(kw, source, score=None):
            if not kw:
                return
            k = kw.strip()
            if len(k) <= 1:
                return
            # If the whole key or all its word tokens are stopwords/banned, skip
            kl = k.lower()
            if kl in custom_stopwords:
                return
            tokens = re.findall(r"\w+", kl)
            if tokens and all(t in custom_stopwords for t in tokens):
                return
            # Convert YAKE score (lower is better) to confidence in [0,1]
            conf = None
            if score is not None:
                try:
                    conf = max(0.0, min(1.0, 1.0 - float(score)))
                except Exception:
                    conf = None
            keywords_detailed.append({"Keyword": k, "Source": source, "ConfidenceScore": conf})
            keywords_for_display.append(k)
        if job_description and job_url not in EXISTING_JOB_URLS:
            if YAKE_EXTRACTOR_DA:
                try:
                    for kw, score in YAKE_EXTRACTOR_DA.extract_keywords(job_description):
                        add_keyword(kw, "yake-da", score)
                except Exception as e:
                    print(f"[Keywords] YAKE Danish extraction failed: {e}")
            if YAKE_EXTRACTOR_EN:
                try:
                    for kw, score in YAKE_EXTRACTOR_EN.extract_keywords(job_description):
                        add_keyword(kw, "yake-en", score)
                except Exception as e:
                    print(f"[Keywords] YAKE English extraction failed: {e}")
            # Add category as a domain keyword with fixed medium confidence
            if category_name and isinstance(category_name, str):
                add_keyword(category_name, "category", 0.5)
            # Deduplicate by keyword, keep highest confidence if multiple
            dedup = {}
            order_index = {}
            for idx, item in enumerate(keywords_detailed):
                key = item["Keyword"].lower()
                existing = dedup.get(key)
                if existing is None:
                    dedup[key] = item
                    order_index[key] = idx
                else:
                    existing_conf = existing["ConfidenceScore"] or 0
                    new_conf = item["ConfidenceScore"] or 0
                    if new_conf > existing_conf:
                        dedup[key] = item
            dedup_items = list(dedup.items())
            dedup_items.sort(
                key=lambda entry, ord_idx=order_index: (
                    (entry[1]["ConfidenceScore"] if entry[1]["ConfidenceScore"] is not None else -1),
                    -ord_idx.get(entry[0], 0)
                ),
                reverse=True
            )
            keywords_detailed = [item for _, item in dedup_items]
            if len(keywords_detailed) > TARGET_KEYWORD_COUNT:
                category_lower = None
                if category_name and isinstance(category_name, str):
                    category_lower = category_name.strip().lower()
                trimmed = keywords_detailed[:TARGET_KEYWORD_COUNT]
                if category_lower and all(k["Keyword"].lower() != category_lower for k in trimmed):
                    category_item = next((k for k in keywords_detailed if k["Keyword"].lower() == category_lower), None)
                    if category_item:
                        trimmed[-1] = category_item
                keywords_detailed = trimmed
            keywords_for_display = [item["Keyword"] for item in keywords_detailed]
        keywords_str = ", ".join(keywords_for_display)

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
            "Keywords": keywords_str,  # kept for display/compatibility
            "KeywordsDetailed": keywords_detailed,
        })
        if job_url:
            EXISTING_JOB_URLS.add(job_url)
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
    # Create JobIndexPostingsExtended table (without embedded keywords)
    create_jobs_table = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='JobIndexPostingsExtended' and xtype='U')
    CREATE TABLE JobIndexPostingsExtended (
        JobID INT IDENTITY(1,1) PRIMARY KEY,
        CompanyName NVARCHAR(255),
        CompanyURL NVARCHAR(MAX),
        JobTitle NVARCHAR(MAX),
        JobLocation NVARCHAR(MAX),
        JobDescription NVARCHAR(MAX),
        JobUrl NVARCHAR(512) UNIQUE,
        Published DATETIME,
        BannerPicture VARBINARY(MAX),
        FooterPicture VARBINARY(MAX),
        SeenLast DATETIME NULL,
        IsActive BIT NOT NULL DEFAULT 1
    )
    """
    # Create Categories table
    create_categories_table = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Categories' and xtype='U')
    CREATE TABLE Categories (
        CategoryID INT IDENTITY(1,1) PRIMARY KEY,
        Name NVARCHAR(255) UNIQUE
    )
    """
    # Create JobCategories join table
    create_jobcategories_table = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='JobCategories' and xtype='U')
    CREATE TABLE JobCategories (
        JobID INT NOT NULL,
        CategoryID INT NOT NULL,
        PRIMARY KEY (JobID, CategoryID),
        FOREIGN KEY (JobID) REFERENCES JobIndexPostingsExtended(JobID) ON DELETE CASCADE,
        FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID) ON DELETE CASCADE
    )
    """
    # Create JobKeywords table
    create_jobkeywords_table = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='JobKeywords' and xtype='U')
    CREATE TABLE JobKeywords (
        KeywordID INT IDENTITY(1,1) PRIMARY KEY,
        JobID INT NOT NULL,
        Keyword NVARCHAR(255) NOT NULL,
        Source NVARCHAR(50) NULL,
        ConfidenceScore FLOAT NULL,
        FOREIGN KEY (JobID) REFERENCES JobIndexPostingsExtended(JobID) ON DELETE CASCADE
    )
    """
    # Ensure JobLocation extends to NVARCHAR(MAX) for existing deployments
    alter_joblocation_column = """
    IF EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'JobIndexPostingsExtended'
          AND COLUMN_NAME = 'JobLocation'
          AND DATA_TYPE = 'nvarchar'
          AND (CHARACTER_MAXIMUM_LENGTH IS NOT NULL AND CHARACTER_MAXIMUM_LENGTH <> -1)
    )
        ALTER TABLE JobIndexPostingsExtended ALTER COLUMN JobLocation NVARCHAR(MAX) NULL
    """
    # Ensure SeenLast column exists
    ensure_seenlast_column = """
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'JobIndexPostingsExtended'
          AND COLUMN_NAME = 'SeenLast'
    )
        ALTER TABLE JobIndexPostingsExtended ADD SeenLast DATETIME NULL
    """
    ensure_isactive_column = """
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'JobIndexPostingsExtended'
          AND COLUMN_NAME = 'IsActive'
    )
        ALTER TABLE JobIndexPostingsExtended ADD IsActive BIT NOT NULL CONSTRAINT DF_JobIndexPostingsExtended_IsActive DEFAULT 1
    """
    backfill_isactive = """
    UPDATE JobIndexPostingsExtended SET IsActive = 1 WHERE IsActive IS NULL
    """
    ensure_seenlast_index = """
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes WHERE name = 'IX_JobIndexPostingsExtended_SeenLast'
          AND object_id = OBJECT_ID('JobIndexPostingsExtended')
    )
        CREATE NONCLUSTERED INDEX IX_JobIndexPostingsExtended_SeenLast ON JobIndexPostingsExtended (SeenLast)
    """
    try:
        cursor.execute(create_jobs_table)
        cursor.execute(create_categories_table)
        cursor.execute(create_jobcategories_table)
        cursor.execute(create_jobkeywords_table)
        cursor.execute(alter_joblocation_column)
        cursor.execute(ensure_seenlast_column)
        cursor.execute(ensure_isactive_column)
        cursor.execute(backfill_isactive)
        cursor.execute(ensure_seenlast_index)
        cursor.commit()
        print("[Main] Database tables checked/created successfully.")
    except Exception as e:
        print(f"[Main] Error setting up database tables: {e}")

# --- Category helper functions ---
def get_or_create_category(cursor, category_name):
    """
    Returns CategoryID for the given name, creating it if it doesn't exist.
    """
    cursor.execute("SELECT CategoryID FROM Categories WHERE Name = ?", category_name)
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("INSERT INTO Categories (Name) VALUES (?)", category_name)
    cursor.execute("SELECT CategoryID FROM Categories WHERE Name = ?", category_name)
    row = cursor.fetchone()
    return row[0] if row else None

def link_job_category(cursor, job_id, category_id):
    """
    Links a job to a category in JobCategories (if not already linked).
    """
    cursor.execute("SELECT 1 FROM JobCategories WHERE JobID = ? AND CategoryID = ?", job_id, category_id)
    if not cursor.fetchone():
        cursor.execute("INSERT INTO JobCategories (JobID, CategoryID) VALUES (?, ?)", job_id, category_id)

def get_jobid_by_url(cursor, job_url):
    cursor.execute("SELECT JobID FROM JobIndexPostingsExtended WHERE JobUrl = ?", job_url)
    row = cursor.fetchone()
    return row[0] if row else None

def insert_keywords(cursor, job_id, keywords_detailed):
    """
    Insert a list of keyword dicts into JobKeywords for the given job_id.
    keywords_detailed: [{"Keyword": str, "Source": str|None, "ConfidenceScore": float|None}, ...]
    """
    if not keywords_detailed:
        return
    try:
        for item in keywords_detailed[:TARGET_KEYWORD_COUNT]:
            cursor.execute(
                "INSERT INTO JobKeywords (JobID, Keyword, Source, ConfidenceScore) VALUES (?, ?, ?, ?)",
                job_id,
                item.get("Keyword"),
                item.get("Source"),
                item.get("ConfidenceScore")
            )
    except Exception as e:
        print(f"[DB Writer] Failed inserting keywords for JobID {job_id}: {e}")


def cleanup_stale_jobs(db_config, days=1):
    """Soft-delete jobs whose SeenLast is older than the given day threshold."""
    try:
        with pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f'SERVER={db_config["server"]};'
            f'DATABASE={db_config["database"]};'
            f'UID={db_config["username"]};'
            f'PWD={db_config["password"]}'
        ) as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(
                """
                UPDATE JobIndexPostingsExtended
                SET IsActive = 0
                WHERE SeenLast < DATEADD(day, -?, GETUTCDATE())
                  AND IsActive = 1
                """,
                days
            )
            affected = cursor.rowcount
            cnxn.commit()
            print(f"[Cleanup] Soft-deactivated {affected if affected is not None else 0} stale jobs (>{days} day(s) old).")
    except Exception as e:
        print(f"[Cleanup] Failed to soft-delete stale jobs: {e}")

def scrape_and_store(start_url, db_config, category):
    """
    Main function to orchestrate scraping from a URL and storing, including pagination
    and asynchronous database writes.
    """
    driver = None
    data_queue = queue.Queue() # Queue for passing job data to the DB writer thread
    db_writer_thread = DatabaseWriter(db_config, data_queue, batch_size=20)
    db_writer_thread.start()
    setup_existing_joburls()  

    page_num = 1
    total_jobs_scraped = 0

    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        user_data_dir = tempfile.mkdtemp(prefix="chrome-user-data-")
        chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
        chrome_options.add_argument("--no-sandbox")
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(start_url)
        print(f"[Main] Navigating to {start_url} (Page {page_num})...")

        # --- Handle Cookie Consent ---
        try:
            print("[Main] Attempting to handle cookie consent...")
            cookie_accept_button = WebDriverWait(driver, 5).until(
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
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[id^='jobad-wrapper-']"))
        )
        print("[Main] Job listings found on page.")

        while True:
            html_content = driver.page_source
            current_page_listings = extract_job_data(html_content, category)
            
            if current_page_listings:
                data_queue.put(current_page_listings)
                # Count only new jobs for total_jobs_scraped
                new_jobs = [j for j in current_page_listings if j["JobUrl"] not in EXISTING_JOB_URLS]
                total_jobs_scraped += len(new_jobs)
                print(f"[Main] Found {len(current_page_listings)} job listings on Page {page_num}. Added to queue. New jobs: {len(new_jobs)}")
            else:
                print(f"[Main] No job listings found on Page {page_num}. This page might be empty or end of results.")


            # --- Pagination Logic ---
            try:
                pagination_ul = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "ul.pagination"))
                )

                next_page_li = pagination_ul.find_element(By.CSS_SELECTOR, "li.page-item-next")
                next_page_link = next_page_li.find_element(By.TAG_NAME, "a")
                next_page_url = next_page_link.get_attribute("href")

                if next_page_url:
                    page_num += 1
                    print(f"[Main] Navigating to next page: {next_page_url} (Page {page_num})...")
                    driver.get(next_page_url)
                    WebDriverWait(driver, 5).until(
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

    # --- spaCy model loading (main thread, pass to workers) ---
    import threading
    import spacy
    from spacy.util import is_package
    try:
        nlp_da = spacy.load("da_core_news_lg")
    except Exception:
        try:
            spacy.cli.download("da_core_news_lg")
            nlp_da = spacy.load("da_core_news_lg")
        except Exception:
            nlp_da = spacy.load("da_core_news_sm")
    try:
        nlp_en = spacy.load("en_core_web_lg")
    except Exception:
        try:
            spacy.cli.download("en_core_web_lg")
            nlp_en = spacy.load("en_core_web_lg")
        except Exception:
            nlp_en = spacy.load("en_core_web_md")

    max_concurrent_threads = 8
    semaphore = threading.Semaphore(max_concurrent_threads)
    threads = []

    def scrape_category_thread(category, url, db_config):
        try:
            print(f"[Main] Starting scrape for category '{category}' with URL: {url}")
            scrape_and_store(url, db_config, category)
        except Exception as e:
            print(f"[Thread] Error in category {category}: {e}")
        finally:
            semaphore.release()

    for category, url in JOBINDEX_URLS.items():
        semaphore.acquire()
        t = threading.Thread(target=scrape_category_thread, args=(category, url, DATABASE_CONFIG))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    cleanup_stale_jobs(DATABASE_CONFIG, days=1)
    print("[Main] All category threads have completed.")