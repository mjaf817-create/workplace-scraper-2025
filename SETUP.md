# Setup

## What you need
- Python 3.8 or newer
- Docker Desktop
- Git

## Getting started

### Clone it
```bash
git clone <repo-url>
cd workplace_scraper
```

### Setup Python environment
```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Mac/Linux
source .venv/bin/activate
```

### Install packages
```bash
pip install -r requirements.txt
```

### Start MongoDB and MinIO
```bash
docker-compose up -d
```

What's running:
- MongoDB on port 27017
- MinIO landing zone on ports 9000/9001
- MinIO curated zone on ports 9002/9003

Login to MinIO consoles with: `minioadmin` / `minioadmin123`

### Run the scraper

**Manual steps:**
```bash
# scrape documents
scrapy crawl workplace_relations -a start_date=2024-10-22 -a end_date=2024-10-31

# download html files
python download_documents.py

# clean and save to curated
python transform_documents.py
```

**Or run everything at once:**
```bash
python orchestrate.py
```

## Custom configuration

everything points to localhost. Change if needed:

**Different MongoDB server:**
```bash
python download_documents.py --mongodb-uri mongodb://your-server:27017/
```

**Different MinIO servers:**
```bash
python download_documents.py --minio-endpoint your-server:9000
python transform_documents.py --landing-minio-endpoint your-server:9000 --curated-minio-endpoint your-server:9002
```

**Different collection names:**
```bash
python download_documents.py --collection test_data
python transform_documents.py --landing-collection test_data --curated-collection test_curated
```

## Check everything works

**MongoDB:**
```bash
python -c "from pymongo import MongoClient; print('connected:', MongoClient('mongodb://localhost:27017/').server_info()['version'])"
```

**MinIO:**
Open http://localhost:9001 in your browser and login with `minioadmin` / `minioadmin123`

## Folder structure
```
workplace_scraper/
├── spiders/               scrapy spiders
├── download_documents.py  grabs html from urls
├── transform_documents.py cleans html
├── config.py              scrapy config
├── pipelines.py           saves to mongodb
├── docker-compose.yml     runs mongo + minio
└── requirements.txt       python packages
```

## Common issues

**Ports already taken**
Change them in docker-compose.yml if 27017, 9000, 9001, 9002, or 9003 are busy

**Can't access MinIO**
Double check login is `minioadmin` / `minioadmin123`

**Scrapy command not found**
Activate the virtual environment first
