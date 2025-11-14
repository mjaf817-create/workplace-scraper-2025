# Workplace Relations Scraper

Scraper for legal decisions from workplacerelations.ie with MongoDB and MinIO storage.

## Overview

Scrapes workplace relations cases (unfair dismissals, discrimination claims, wage disputes, etc.) and stores them in MongoDB with HTML files in MinIO. 

The pipeline cleans HTML by removing navigation, ads, and scripts while preserving case content.

Pipeline:
1. **Scrape** - crawl site and extract case metadata
2. **Download** - fetch HTML files to MinIO landing zone
3. **Transform** - clean HTML and move to curated zone

## Setup

```bash
git clone https://github.com/mjaf817-create/workplace-scraper-2025.git
cd workplace_scraper

python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

docker-compose up -d

python src/orchestrate.py
```

## Files

- `src/scrapers/spiders/workplace_spider.py` - crawls the site
- `src/utils/download_documents.py` - grabs HTML files
- `src/utils/transform_documents.py` - cleans the HTML
- `src/orchestrate.py` - runs all three steps
- `src/dashboard.py` - monitoring dashboard
- `src/utils/query_data.py` - check stats
- `src/scrapers/config.py` - scrapy settings
- `src/scrapers/pipelines.py` - saves to mongodb
- `src/scrapers/items.py` - data schema

## Storage

**MongoDB** (localhost:27017)
- `decisions` - raw metadata
- `decisions_curated` - cleaned metadata

**MinIO Landing** (localhost:9000)
- original HTML

**MinIO Curated** (localhost:9002)
- cleaned HTML

## HTML Cleaning

Removes:
- Navigation, headers, footers
- Forms, buttons, input fields
- Ads, cookie notices, social widgets
- Scripts, styles, images
- HTML comments
- Empty tags
- Excess whitespace
- Inline styles

Preserves:
- Case text
- Tables
- Lists

## Custom Date Ranges

```bash
scrapy crawl workplace_relations -a start_date=2024-10-22 -a end_date=2024-10-31

python src/utils/download_documents.py --collection test_data
python src/utils/transform_documents.py --landing-collection test_data --curated-collection test_curated
```

## Query Data

```bash
python src/utils/query_data.py
```

Shows counts for scraped, downloaded, and transformed documents.

## Dashboard

```bash
streamlit run src/dashboard.py
```

http://localhost:8501

Features:
- Pipeline stage counts
- MongoDB/MinIO sync status
- Funnel charts, monthly trends
- Recent documents
- 30s auto-refresh

## Stack

- **Scrapy** - web scraping
- **MongoDB** - metadata storage
- **MinIO** - object storage
- **BeautifulSoup** - HTML parsing
- **Docker** - infrastructure

## Structure

```
workplace_scraper/
├── src/
│   ├── scrapers/
│   │   ├── spiders/
│   │   │   └── workplace_spider.py
│   │   ├── config.py
│   │   ├── items.py
│   │   └── pipelines.py
│   ├── utils/
│   │   ├── download_documents.py
│   │   ├── transform_documents.py
│   │   └── query_data.py
│   ├── orchestrate.py
│   └── dashboard.py
├── docker-compose.yml
└── requirements.txt
```

