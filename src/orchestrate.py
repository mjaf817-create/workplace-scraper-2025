"""
runs the full pipeline: scrape -> download -> transform

optionally start dashboard with --dashboard flag
"""

import subprocess
import sys
from datetime import datetime, timedelta

def run_command(cmd, description):
    print(f"\n{description}...")
    
    result = subprocess.run(cmd, shell=True)
    
    if result.returncode != 0:
        print(f"\nError in: {description}")
        print("Check logs for details")
        sys.exit(1)
    
    print(f"Completed {description}")
    return True

def main():
    start_dashboard = '--dashboard' in sys.argv
    
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print("Workplace Relations Pipeline")
    
    print(f"\nProcessing documents from: {yesterday}")
    if start_dashboard:
        print("Dashboard will launch after pipeline completes")
    
    scrape_cmd = f"scrapy crawl workplace_relations -a start_date={yesterday} -a end_date={yesterday}"
    run_command(scrape_cmd, "scraping website")
    
    download_cmd = "python src/utils/download_documents.py"
    run_command(download_cmd, "downloading HTML files")
    
    transform_cmd = "python src/utils/transform_documents.py"
    run_command(transform_cmd, "transforming documents")
    print("Pipeline complete")
    
    
    if start_dashboard:
        print("\nLaunching dashboard")
        print("Opening at: http://localhost:8501")
        print("Press Ctrl+C to stop\n")
        try:
            subprocess.run([sys.executable, "-m", "streamlit", "run", "src/dashboard.py"])
        except KeyboardInterrupt:
            print("\nStopping dashboard")
            sys.exit(0)


if __name__ == '__main__':
    main()
