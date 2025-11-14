"""transform documents from landing to curated zone"""

import os
import hashlib
import logging
from datetime import datetime
from io import BytesIO
from pymongo import MongoClient
from minio import Minio
from minio.error import S3Error
from bs4 import BeautifulSoup
import re

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('transform_documents.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DocumentTransformer:
    def __init__(self,
                 mongodb_uri='mongodb://localhost:27017/',
                 landing_minio_endpoint='localhost:9000',
                 curated_minio_endpoint='localhost:9002'):
        
        self.mongo_client = MongoClient(mongodb_uri)
        self.db = self.mongo_client['workplace_relations']
        self.landing_collection = self.db['decisions']
        self.curated_collection = self.db['decisions_curated']
        
        self.landing_minio_client = Minio(
            landing_minio_endpoint,
            access_key='minioadmin',
            secret_key='minioadmin123',
            secure=False
        )
        
        self.curated_minio_client = Minio(
            curated_minio_endpoint,
            access_key='minioadmin',
            secret_key='minioadmin123',
            secure=False
        )
        
        self.landing_bucket = 'landing-zone'
        self.curated_bucket = 'curated-zone'
        self.ensure_bucket(self.curated_bucket)
        
        logger.info("DocumentTransformer initialized")
        logger.info(f"Landing: {landing_minio_endpoint}/{self.landing_bucket} -> Curated: {curated_minio_endpoint}/{self.curated_bucket}")
    
    def ensure_bucket(self, bucket_name):
        """create bucket if needed"""
        try:
            if not self.curated_minio_client.bucket_exists(bucket_name):
                self.curated_minio_client.make_bucket(bucket_name)
                logger.info(f"created {bucket_name}")
        except S3Error as e:
            logger.error(f"bucket error: {e}")
            raise
    
    def calc_hash(self, content):
        """sha256 hash"""
        if isinstance(content, str):
            content = content.encode('utf-8')
        return hashlib.sha256(content).hexdigest()
    
    def get_file(self, bucket, object_name, from_landing=True):
        """get file from minio"""
        try:
            client = self.landing_minio_client if from_landing else self.curated_minio_client
            response = client.get_object(bucket, object_name)
            content = response.read()
            response.close()
            response.release_conn()
            return content
        except S3Error as e:
            logger.error(f"get failed {object_name}: {e}")
            return None
    
    def clean_html(self, html_content):
        """extract main content from html"""
        soup = BeautifulSoup(html_content, 'lxml')
        
        for tag in soup(['nav', 'header', 'footer', 'aside', 'script', 'style', 'noscript', 'iframe', 'svg', 'canvas']):
            tag.decompose()
        
        for tag in soup.find_all(['button', 'form', 'input', 'select', 'textarea']):
            tag.decompose()
        
        noise_patterns = ['nav', 'menu', 'sidebar', 'advertisement', 'ad-', 'cookie', 'banner', 'social', 'share']
        for pattern in noise_patterns:
            for tag in soup.find_all(class_=lambda x: x and pattern in x.lower()):
                tag.decompose()
            for tag in soup.find_all(id=lambda x: x and pattern in x.lower()):
                tag.decompose()
        
        from bs4 import Comment
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()
        
        main = (soup.find('main') or 
                soup.find(id='main-content') or 
                soup.find(class_='content') or 
                soup.find('article') or 
                soup.find('body') or 
                soup)
        
        if main:
            for tag in main.find_all(style=True):
                del tag['style']
            
            for tag in main.find_all():
                if not tag.get_text(strip=True) and not tag.find_all(['img', 'br', 'hr', 'input']) and tag.name not in ['br', 'hr']:
                    tag.decompose()
            
            for br in main.find_all('br'):
                next_sibling = br.next_sibling
                if next_sibling and next_sibling.name == 'br':
                    count = 0
                    current = br
                    while current and current.name == 'br':
                        count += 1
                        if count > 2:
                            next_br = current.next_sibling
                            current.decompose()
                            current = next_br
                        else:
                            current = current.next_sibling
            
            for text in main.find_all(string=True):
                if text.parent.name not in ['script', 'style', 'table', 'thead', 'tbody', 'tfoot', 'tr', 'td', 'th', 'ul', 'ol', 'li']:
                    cleaned = ' '.join(text.split())
                    text.replace_with(cleaned)
        
        return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>Decision</title></head>
<body>{str(main)}</body>
</html>"""
    
    def save_to_curated(self, content, identifier, extension, original_path=None):
        """save file to curated minio"""
        if original_path and '/' in original_path:
            folder = original_path.rsplit('/', 1)[0]
            object_name = f"{folder}/{identifier}.{extension}"
        else:
            object_name = f"{identifier}.{extension}"
        
        if isinstance(content, str):
            content = content.encode('utf-8')
        
        try:
            self.curated_minio_client.put_object(
                self.curated_bucket,
                object_name,
                BytesIO(content),
                len(content),
                self.get_mime_type(extension)
            )
            logger.info(f"stored {identifier}")
            return object_name
        except S3Error as e:
            logger.error(f"store failed: {e}")
            return None
    
    def get_mime_type(self, extension):
        """mime type for file extension"""
        types = {
            'pdf': 'application/pdf',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'html': 'text/html'
        }
        return types.get(extension, 'text/html')
    
    def process_doc(self, landing_doc):
        """transform single document"""
        identifier = landing_doc.get('identifier', '')
        file_path = landing_doc.get('file_path', '')
        
        if not file_path:
            logger.warning(f"no file: {identifier}")
            return False
        
        content = self.get_file(self.landing_bucket, file_path)
        
        if content is None:
            logger.error(f"get failed: {file_path}")
            return False
        
        extension = file_path.split('.')[-1].lower()
        
        if extension == 'html':
            transformed_content = self.clean_html(content)
        else:
            transformed_content = content
        
        new_hash = self.calc_hash(transformed_content)
        
        curated_path = self.save_to_curated(transformed_content, identifier, extension, original_path=file_path)
        
        if not curated_path:
            return False
        
        curated_doc = landing_doc.copy()
        curated_doc['file_path'] = curated_path
        curated_doc['file_hash'] = new_hash
        curated_doc['transformed_at'] = datetime.now().isoformat()
        curated_doc['source_file_path'] = file_path
        curated_doc['source_file_hash'] = landing_doc.get('file_hash', '')
        
        try:
            self.curated_collection.replace_one(
                {'_id': curated_doc['_id']},
                curated_doc,
                upsert=True
            )
            logger.info(f"done: {identifier}")
            return True
        except Exception as e:
            logger.error(f"save failed: {e}")
            return False
    
    def run(self, start_date=None, end_date=None, limit=None, skip_existing=True):
        """transform documents from landing to curated"""
        query = {'file_path': {'$exists': True}}
        
        if skip_existing:
            curated_ids = {doc['_id'] for doc in self.curated_collection.find({}, {'_id': 1})}
            if curated_ids:
                query['_id'] = {'$nin': list(curated_ids)}
        
        if start_date or end_date:
            date_query = {}
            if start_date:
                date_query['$gte'] = start_date
            if end_date:
                date_query['$lte'] = end_date
            query['published_date'] = date_query
        
        cursor = self.landing_collection.find(query)
        if limit:
            cursor = cursor.limit(limit)
        
        total = self.landing_collection.count_documents(query)
        logger.info(f"{total} docs to transform")
        
        success = 0
        
        for idx, doc in enumerate(cursor, 1):
            logger.info(f"[{idx}/{total}] {doc.get('identifier', 'unknown')}")
            if self.process_doc(doc):
                success += 1
        
        logger.info(f"done: {success}/{total}")
        return success, total - success
    
    def stats(self):
        """get stats"""
        landing_with_files = self.landing_collection.count_documents({'file_path': {'$exists': True}})
        curated_total = self.curated_collection.count_documents({})
        pending = landing_with_files - curated_total
        
        return {
            'landing_with_files': landing_with_files,
            'curated_total': curated_total,
            'pending': pending,
            'percentage': (curated_total / landing_with_files * 100) if landing_with_files > 0 else 0
        }
    
    def close(self):
        self.mongo_client.close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='transform workplace docs')
    parser.add_argument('--start-date', help='start date dd/mm/yyyy')
    parser.add_argument('--end-date', help='end date dd/mm/yyyy')
    parser.add_argument('--limit', type=int, help='limit docs')
    parser.add_argument('--skip-existing', action='store_true', default=True)
    parser.add_argument('--stats', action='store_true', help='show stats')
    parser.add_argument('--mongodb-uri', default='mongodb://localhost:27017/')
    parser.add_argument('--landing-minio-endpoint', default='localhost:9000')
    parser.add_argument('--curated-minio-endpoint', default='localhost:9002')
    
    args = parser.parse_args()
    
    transformer = DocumentTransformer(
        mongodb_uri=args.mongodb_uri,
        landing_minio_endpoint=args.landing_minio_endpoint,
        curated_minio_endpoint=args.curated_minio_endpoint
    )
    
    try:
        if args.stats:
            stats = transformer.stats()
            print(f"\n{stats['curated_total']}/{stats['landing_with_files']} transformed ({stats['percentage']:.1f}%)")
            print(f"Pending: {stats['pending']:,}\n")
        else:
            success, errors = transformer.run(skip_existing=False)
            print(f'\nComplete: {success} transformed, {errors} errors')
    finally:
        transformer.close()


if __name__ == '__main__':
    main()
