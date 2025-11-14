#!/bin/bash

# Setup script for Workplace Relations Scraper

echo "=========================================="
echo "Setting up Workplace Relations Scraper"
echo "=========================================="
echo ""

# Check Python version
echo "Checking Python version..."
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo "Python $PYTHON_VERSION found"
echo ""

# Install dependencies
echo "Installing Python dependencies..."
pip3 install -r requirements.txt
echo "Dependencies installed"
echo ""

# Check MongoDB
echo "Checking MongoDB installation..."
if command -v mongosh &> /dev/null; then
    echo "MongoDB client found"
    
    # Try to connect
    if mongosh --eval "db.version()" > /dev/null 2>&1; then
        echo "MongoDB is running"
        MONGO_VERSION=$(mongosh --eval "db.version()" --quiet)
        echo "  Version: $MONGO_VERSION"
    else
        echo "MongoDB is not running"
        echo ""
        echo "To start MongoDB:"
        echo "  Ubuntu/Debian: sudo systemctl start mongodb"
        echo "  macOS: brew services start mongodb-community"
        echo "  Docker: docker run -d -p 27017:27017 --name mongodb mongo:latest"
    fi
else
    echo "MongoDB not found"
    echo ""
    echo "To install MongoDB:"
    echo "  Ubuntu/Debian: sudo apt-get install mongodb"
    echo "  macOS: brew install mongodb-community"
    echo "  Docker: docker run -d -p 27017:27017 --name mongodb mongo:latest"
fi

echo ""
echo "=========================================="
echo "Setup complete!"
echo ""
echo "To start scraping, run:"
echo "  ./run_scraper.sh 2024-01-01 2025-01-01 monthly"
echo ""
echo "Or use scrapy directly:"
echo "  scrapy crawl workplace_relations -a start_date=2024-01-01 -a end_date=2025-01-01"
echo "=========================================="
