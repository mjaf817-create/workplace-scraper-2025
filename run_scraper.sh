#!/bin/bash

# Workplace Relations Scraper - Quick Start Script

echo "=========================================="
echo "Workplace Relations Scraper"
echo "=========================================="
echo ""

# Default values
START_DATE="${1:-2024-01-01}"
END_DATE="${2:-2025-01-01}"
PARTITION="${3:-monthly}"

echo "Configuration:"
echo "  Start Date: $START_DATE"
echo "  End Date: $END_DATE"
echo "  Partition: $PARTITION"
echo ""

# Check if MongoDB is running
echo "Checking MongoDB connection..."
if ! mongosh --eval "db.version()" > /dev/null 2>&1; then
    echo "Warning: Could not connect to MongoDB"
    echo "   Make sure MongoDB is running:"
    echo "   - Ubuntu/Debian: sudo systemctl start mongodb"
    echo "   - macOS: brew services start mongodb-community"
    echo "   - Docker: docker run -d -p 27017:27017 --name mongodb mongo:latest"
    echo ""
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    echo "MongoDB is running"
fi

echo ""
echo "Starting scraper..."
echo "=========================================="
echo ""

# Run the scraper
scrapy crawl workplace_relations \
    -a start_date="$START_DATE" \
    -a end_date="$END_DATE" \
    -a partition="$PARTITION"

echo ""
echo "=========================================="
echo "Scraping completed!"
echo ""
echo "To view results in MongoDB:"
echo "  mongosh"
echo "  use workplace_relations"
echo "  db.decisions.countDocuments()"
echo "=========================================="
