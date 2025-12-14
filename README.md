# JobScraper

JobScraper is a Python script that scrapes job postings from [jobindex.dk](https://www.jobindex.dk) across multiple categories and stores the results in a Microsoft SQL Server database. It uses Selenium for web automation, BeautifulSoup for HTML parsing, and supports asynchronous database writes for efficiency.

## Features

- Scrapes job postings from all subcategories (`subid=1` to `subid=150`) on jobindex.dk.
- Extracts company name, job title, location, description, posting date, images, and more.
- Handles cookie consent and modal dialogs automatically.
- Stores results in a SQL Server database, creating the table if it does not exist.
- Downloads and stores banner and footer images as binary data.
- Uses a separate thread for database writes to improve performance.

## Requirements

- Python 3.7+
- Google Chrome browser
- ChromeDriver (compatible with your Chrome version)
- Microsoft SQL Server (with network access)
- ODBC Driver 17 for SQL Server

### Python Packages


Install dependencies with:

```sh
pip install selenium beautifulsoup4 requests pyodbc spacy yake rake-nltk
```

For best accuracy, the script will attempt to use the largest available spaCy models for Danish and English. You may want to pre-download them:

```sh
python -m spacy download da_core_news_md
python -m spacy download en_core_web_lg
```

## Configuration

The script uses two configuration files:

- `config.ini` – Default configuration (template provided)
- `config.development.ini` – Development/override configuration (ignored by git)

Example `config.ini`:

```ini
[database]
server = server
database = database
username = username
password = password
```

Example `config.development.ini` (not tracked by git):

```ini
[database]
server = your_server
database = your_database
username = your_username
password = your_password
```

## Usage

1. Ensure your database and ODBC driver are set up.
2. Update `config.ini` or create a `config.development.ini` with your database credentials.
3. Download the correct version of ChromeDriver and ensure it is in your PATH.
4. Run the script:

```sh
python scraper.py
```


The script will iterate through all job categories and store job postings in the `JobIndexPostingsExtended` table.

### Advanced Keyword Extraction

- Uses the best available spaCy models for Danish and English (NER and noun chunks)
- Integrates [YAKE](https://github.com/LIAAD/yake) for single-word keywords (Danish and English)
- Uses the job category as a domain-specific keyword
- Extracted keywords are now stored in a separate `JobKeywords` table with source and confidence score; a human-readable comma-separated list is still kept in-memory for display but no longer stored in `JobIndexPostingsExtended`

## Database Tables

The script will automatically create these tables if they do not exist:

JobIndexPostingsExtended

- JobID INT (PK)
- CompanyName NVARCHAR(255)
- CompanyURL NVARCHAR(MAX)
- JobTitle NVARCHAR(MAX)
- JobLocation NVARCHAR(MAX)
- JobDescription NVARCHAR(MAX)
- JobUrl NVARCHAR(512) UNIQUE
- Published DATETIME
- BannerPicture VARBINARY(MAX)
- FooterPicture VARBINARY(MAX)
- SeenLast DATETIME NULL

Categories

- CategoryID INT (PK)
- Name NVARCHAR(255) UNIQUE

JobCategories (join)

- JobID INT (FK -> JobIndexPostingsExtended.JobID)
- CategoryID INT (FK -> Categories.CategoryID)
- PRIMARY KEY (JobID, CategoryID)

JobKeywords

- KeywordID INT (PK)
- JobID INT (FK -> JobIndexPostingsExtended.JobID)
- Keyword NVARCHAR(255) NOT NULL
- Source NVARCHAR(50) NULL
- ConfidenceScore FLOAT NULL

## Migration Notes

- If you previously had a `Keywords` column in `JobIndexPostingsExtended`, it is no longer used by this script. You may optionally migrate legacy values into `JobKeywords` by splitting on commas and inserting rows with `Source='legacy'` and `ConfidenceScore=NULL`.
- Example T-SQL (optional):

```
-- One-time migration example: split comma-separated legacy keywords
-- Adjust STRING_SPLIT usage as needed for your SQL Server version
INSERT INTO JobKeywords (JobID, Keyword, Source, ConfidenceScore)
SELECT j.JobID, LTRIM(RTRIM(value)) AS Keyword, 'legacy' AS Source, NULL AS ConfidenceScore
FROM JobIndexPostingsExtended j
CROSS APPLY STRING_SPLIT(COALESCE(j.Keywords, ''), ',') s
WHERE LTRIM(RTRIM(value)) <> '';
```

## Notes

- Each scrape updates the `SeenLast` timestamp so you can deactivate rows that go stale (`UPDATE JobIndexPostingsExtended SET IsActive = 0 WHERE SeenLast < DATEADD(day, -30, GETUTCDATE())`, etc.).
- The script is designed for educational and research purposes. Please respect the terms of service of jobindex.dk.
- For large-scale scraping, consider adding delays or rate limiting to avoid overloading the target site.

## License

MIT License
