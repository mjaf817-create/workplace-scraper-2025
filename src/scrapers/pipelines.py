import pymongo
from datetime import datetime


class MongoPipeline:
    
    def __init__(self, uri, database, collection):
        self.uri = uri
        self.database = database
        self.collection_name = collection
        self.client = None
        self.db = None
        self.collection = None
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            uri=crawler.settings.get('MONGODB_URI', 'mongodb://localhost:27017/'),
            database=crawler.settings.get('MONGODB_DATABASE', 'workplace_relations'),
            collection=crawler.settings.get('MONGODB_COLLECTION', 'decisions')
        )
    
    def open_spider(self, spider):
        try:
            self.client = pymongo.MongoClient(self.uri)
            self.db = self.client[self.database]
            self.collection = self.db[self.collection_name]
            
            self.collection.create_index([('identifier', pymongo.ASCENDING)], unique=True)
            self.collection.create_index([('published_date', pymongo.DESCENDING)])
            self.collection.create_index([('partition_date', pymongo.ASCENDING)])
            
            spider.logger.info(f"mongo: {self.database}.{self.collection_name}")
        except Exception as e:
            spider.logger.error(f"mongo error: {e}")
            raise
    
    def close_spider(self, spider):
        if self.client:
            self.client.close()
    
    def process_item(self, item, spider):
        try:
            item_dict = dict(item)
            
            if 'scraped_at' not in item_dict:
                item_dict['scraped_at'] = datetime.now().isoformat()
            
            self.collection.update_one(
                {'identifier': item_dict['identifier']},
                {'$set': item_dict},
                upsert=True
            )
            
        except Exception as e:
            spider.logger.error(f"error: {e}")
        
        return item
