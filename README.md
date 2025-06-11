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
pip install selenium beautifulsoup4 requests pyodbc
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

The script will iterate through all job categories and store job postings in the `JobIndexPostings` table.

## Database Table

The script will automatically create the following table if it does not exist:

| Column         | Type            | Description                |
|----------------|-----------------|----------------------------|
| JobID          | INT (PK)        | Auto-incremented ID        |
| CompanyName    | NVARCHAR(255)   | Name of the company        |
| CompanyURL     | NVARCHAR(MAX)   | URL to the company         |
| JobTitle       | NVARCHAR(MAX)   | Title of the job           |
| JobLocation    | NVARCHAR(255)   | Location of the job        |
| JobDescription | NVARCHAR(MAX)   | Description of the job     |
| JobUrl         | NVARCHAR(512)   | Unique URL of the posting  |
| Published      | DATETIME        | Date published             |
| Category       | NVARCHAR(255)   | Job category               |
| BannerPicture  | VARBINARY(MAX)  | Banner image (if any)      |
| FooterPicture  | VARBINARY(MAX)  | Footer image (if any)      |

## Notes

- The script is designed for educational and research purposes. Please respect the terms of service of jobindex.dk.
- For large-scale scraping, consider adding delays or rate limiting to avoid overloading the target site.

## License

MIT License
