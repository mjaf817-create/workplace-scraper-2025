#!/usr/bin/env python3
"""
query and export data from mongodb
"""

import pymongo
import json
import csv
from datetime import datetime
import argparse


class WorkplaceDataQuery:
    def __init__(self, uri='mongodb://localhost:27017/', 
                 database='workplace_relations', 
                 collection='decisions'):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[database]
        self.collection = self.db[collection]
    
    def get_stats(self):
        """stats about scraped data"""
        total = self.collection.count_documents({})
        print(f"\n{total:,} total")
        
        print("\nby body:")
        body_stats = self.collection.aggregate([
            {"$group": {"_id": "$body", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ])
        for stat in body_stats:
            print(f"  {stat['_id']}: {stat['count']:,}")
        
        print("\nby partition:")
        partition_stats = self.collection.aggregate([
            {"$group": {"_id": "$partition_date", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ])
        for stat in partition_stats:
            print(f"  {stat['_id']}: {stat['count']:,}")
        
        print("\nlatest:")
        latest = self.collection.find().sort("scraped_at", -1).limit(5)
        for doc in latest:
            print(f"  {doc.get('identifier', 'N/A')} - {doc.get('description', 'N/A')[:60]}...")
        
        print()
    
    def export_to_json(self, filename=None):
        """export to json"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'workplace_decisions_{timestamp}.json'
        
        data = list(self.collection.find({}, {'_id': 0}))
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"{len(data):,} records -> {filename}")
        return filename
    
    def export_to_csv(self, filename=None):
        """export to csv"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'workplace_decisions_{timestamp}.csv'
        
        data = list(self.collection.find({}, {'_id': 0}))
        
        if not data:
            print("no data")
            return None
        
        fields = set()
        for doc in data:
            fields.update(doc.keys())
        fields = sorted(fields)
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(data)
        
        print(f"{len(data):,} records -> {filename}")
        return filename
    
    def search(self, query_text=None, body=None, start_date=None, end_date=None, limit=10):
        """search decisions"""
        query = {}
        
        if query_text:
            query['$or'] = [
                {'description': {'$regex': query_text, '$options': 'i'}},
                {'identifier': {'$regex': query_text, '$options': 'i'}}
            ]
        
        if body:
            query['body'] = body
        
        if start_date or end_date:
            query['partition_date'] = {}
            if start_date:
                query['partition_date']['$gte'] = start_date
            if end_date:
                query['partition_date']['$lte'] = end_date
        
        results = self.collection.find(query, {'_id': 0}).limit(limit)
        
        print(f"\nresults (max {limit}):\n")
        
        count = 0
        for doc in results:
            count += 1
            print(f"{count}. {doc.get('identifier', 'N/A')}")
            print(f"   {doc.get('description', 'N/A')[:80]}")
            print(f"   {doc.get('body', 'N/A')} - {doc.get('published_date', 'N/A')}\n")
        
        if count == 0:
            print("no results\n")
        
        return count
    
    def close(self):
        self.client.close()


def main():
    parser = argparse.ArgumentParser(description='query workplace relations data')
    parser.add_argument('--stats', action='store_true', help='show stats')
    parser.add_argument('--export-json', action='store_true', help='export to json')
    parser.add_argument('--export-csv', action='store_true', help='export to csv')
    parser.add_argument('--search', type=str, help='search text')
    parser.add_argument('--body', type=str, help='filter by body')
    parser.add_argument('--limit', type=int, default=10, help='limit results')
    parser.add_argument('--uri', type=str, default='mongodb://localhost:27017/', 
                        help='mongodb uri')
    
    args = parser.parse_args()
    
    try:
        query = WorkplaceDataQuery(uri=args.uri)
        
        if args.stats:
            query.get_stats()
        
        if args.export_json:
            query.export_to_json()
        
        if args.export_csv:
            query.export_to_csv()
        
        if args.search:
            query.search(query_text=args.search, body=args.body, limit=args.limit)
        
        if not any([args.stats, args.export_json, args.export_csv, args.search]):
            query.get_stats()
        
        query.close()
        
    except Exception as e:
        print(f"error: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())

