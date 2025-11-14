"""download html/pdf files and save to minio"""

import os
import hashlib
import logging
from datetime import datetime
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from minio import Minio
from minio.error import S3Error
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('download_documents.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class Downloader:
    def __init__(self, mongodb_uri='mongodb://localhost:27017/',
                 minio_endpoint='localhost:9000'):
        
        self.mongo_client = MongoClient(mongodb_uri)
        self.db = self.mongo_client['workplace_relations']
        self.collection = self.db['decisions']
        
        self.minio_client = Minio(
            minio_endpoint,
            access_key='minioadmin',
            secret_key='minioadmin123',
            secure=False
        )
        
        self.bucket = 'landing-zone'
        self.ensure_bucket()
        
        logger.info(f"downloader ready: {minio_endpoint}")
    
    def ensure_bucket(self):
        try:
            if not self.minio_client.bucket_exists(self.bucket):
                self.minio_client.make_bucket(self.bucket)
                logger.info(f"created {self.bucket}")
        except S3Error as e:
            logger.error(f"bucket error: {e}")
            raise
    
    def calc_hash(self, content):
        return hashlib.sha256(content).hexdigest()
    
    def fetch(self, url, timeout=30):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response.content, response.headers.get('Content-Type', '')
        except requests.exceptions.RequestException as e:
            logger.error(f"failed {url}: {e}")
            return None, None
    
    def guess_extension(self, url, content_type):
        path = urlparse(url).path.lower()
        
        if path.endswith('.pdf') or 'pdf' in content_type.lower():
            return 'pdf'
        elif path.endswith(('.doc', '.docx')) or 'word' in content_type.lower():
            return 'docx'
        
        return 'html'
    
    def save_to_minio(self, content, identifier, extension, partition_date):
        from io import BytesIO
        
        object_name = f"{partition_date}/{identifier}.{extension}"
        mime_types = {
            'pdf': 'application/pdf',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'html': 'text/html'
        }
        
        try:
            self.minio_client.put_object(
                self.bucket,
                object_name,
                BytesIO(content),
                len(content),
                mime_types.get(extension, 'text/html')
            )
            logger.info(f"saved {identifier}")
            return object_name
        except S3Error as e:
            logger.error(f"save failed: {e}")
            return None
    
    def update_mongo(self, doc_id, file_path, file_hash):
        try:
            self.collection.update_one(
                {'_id': doc_id},
                {'$set': {
                    'file_path': file_path,
                    'file_hash': file_hash,
                    'downloaded_at': datetime.now().isoformat()
                }}
            )
        except Exception as e:
            logger.error(f"update failed: {e}")
    
    def run(self, start_date=None, end_date=None, limit=None, skip_existing=True):
        query = {}
        
        if skip_existing:
            query['file_path'] = {'$exists': False}
        
        if start_date or end_date:
            date_query = {}
            if start_date:
                date_query['$gte'] = start_date
            if end_date:
                date_query['$lte'] = end_date
            query['published_date'] = date_query
        
        cursor = self.collection.find(query)
        if limit:
            cursor = cursor.limit(limit)
        
        total = self.collection.count_documents(query)
        logger.info(f"{total} docs to download")
        
        success = 0
        errors = 0
        
        for idx, doc in enumerate(cursor, 1):
            identifier = doc.get('identifier', '')
            link = doc.get('link_to_doc', '')
            partition_date = doc.get('partition_date', 'unknown')
            
            if not link:
                logger.warning(f"no link: {identifier}")
                errors += 1
                continue
            
            logger.info(f"[{idx}/{total}] {identifier}")
            
            content, content_type = self.fetch(link)
            if content is None:
                errors += 1
                continue
            
            extension = self.guess_extension(link, content_type)
            file_hash = self.calc_hash(content)
            file_path = self.save_to_minio(content, identifier, extension, partition_date)
            
            if file_path:
                self.update_mongo(doc['_id'], file_path, file_hash)
                success += 1
            else:
                errors += 1
            
            time.sleep(1.5)
        
        logger.info(f"done: {success}/{total}")
        return success, errors
    
    def stats(self):
        total = self.collection.count_documents({})
        downloaded = self.collection.count_documents({'file_path': {'$exists': True}})
        pending = total - downloaded
        
        return {
            'total': total,
            'downloaded': downloaded,
            'pending': pending,
            'pct': (downloaded / total * 100) if total > 0 else 0
        }
    
    def close(self):
        self.mongo_client.close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='download docs')
    parser.add_argument('--start-date', help='start date dd/mm/yyyy')
    parser.add_argument('--end-date', help='end date dd/mm/yyyy')
    parser.add_argument('--limit', type=int, help='max docs')
    parser.add_argument('--skip-existing', action='store_true', default=True)
    parser.add_argument('--stats', action='store_true')
    parser.add_argument('--mongodb-uri', default='mongodb://localhost:27017/')
    parser.add_argument('--minio-endpoint', default='localhost:9000')
    
    args = parser.parse_args()
    
    downloader = Downloader(
        mongodb_uri=args.mongodb_uri,
        minio_endpoint=args.minio_endpoint
    )
    
    try:
        if args.stats:
            s = downloader.stats()
            print(f"\n{s['downloaded']}/{s['total']} downloaded ({s['pct']:.1f}%)")
            print(f"Pending: {s['pending']:,}\n")
        else:
            downloader.run(
                start_date=args.start_date,
                end_date=args.end_date,
                limit=args.limit,
                skip_existing=args.skip_existing
            )
    finally:
        downloader.close()


if __name__ == '__main__':
    main()
