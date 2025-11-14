import scrapy
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
from src.scrapers.items import WorkplaceDecisionItem

DEBUG_DIR = Path('debug')
DEBUG_DIR.mkdir(exist_ok=True)


class WorkplaceSpider(scrapy.Spider):
    name = 'workplace_relations'
    allowed_domains = ['workplacerelations.ie']
    start_urls = ['https://www.workplacerelations.ie/en/search/?advance=true']
    
    IDENTIFIER_PATTERNS = r'(ADJ-\d+|DWT\d+|UDD\d+|EAT-\d+|IR-SC-\d+)'
    
    def __init__(self, *args, start_date=None, end_date=None, partition='monthly', **kwargs):
        super(WorkplaceSpider, self).__init__(*args, **kwargs)
        
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d') if start_date else datetime(2024, 1, 1)
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d') if end_date else datetime.now()
        self.partition = partition
        self.logger.info(f"{self.start_date.date()} to {self.end_date.date()} ({self.partition})")
        
        self.date_partitions = self._generate_partitions()
        self.logger.info(f"{len(self.date_partitions)} partitions")
    
    def _generate_partitions(self):
        partitions = []
        current = self.start_date
        
        while current < self.end_date:
            if self.partition == 'monthly':
                end = current + relativedelta(months=1) - timedelta(days=1)
                next_start = current + relativedelta(months=1)
            elif self.partition == 'weekly':
                end = current + timedelta(days=6)
                next_start = current + timedelta(days=7)
            else:  # daily
                end = current
                next_start = current + timedelta(days=1)
            
            if end > self.end_date:
                end = self.end_date
            
            partitions.append({
                'start': current,
                'end': end,
                'partition_label': current.strftime('%Y-%m')
            })
            
            current = next_start
        
        return partitions
    
    def start_requests(self):
        for partition in self.date_partitions:
            yield scrapy.Request(
                url=self.start_urls[0],
                callback=self.submit_search,
                meta={
                    'start_date': partition['start'],
                    'end_date': partition['end'],
                    'partition_label': partition['partition_label']
                },
                dont_filter=True
            )
    
    def submit_search(self, response):
        start_date = response.meta['start_date']
        end_date = response.meta['end_date']
        partition_label = response.meta['partition_label']
        
        self.logger.info(f"{start_date.date()} to {end_date.date()}")
        
        start_date_str = start_date.strftime('%d/%m/%Y').lstrip('0').replace('/0', '/')
        end_date_str = end_date.strftime('%d/%m/%Y').lstrip('0').replace('/0', '/') 
        
        search_url = f'https://www.workplacerelations.ie/en/search/?decisions=1&from={start_date_str}&to={end_date_str}&pageNumber=1'
        
        yield scrapy.Request(
            url=search_url,
            callback=self.parse,
            meta={
                'partition_label': partition_label,
                'start_date': start_date,
                'end_date': end_date,
                'page': 1
            },
            dont_filter=True
        )
    
    def parse(self, response):
        partition_label = response.meta['partition_label']
        page = response.meta.get('page', 1)
        
        self.logger.info(f"{partition_label} p{page}")
        
        results = response.css('li.each-item')
        if not results:
            results = response.xpath('//div[contains(., "ADJ-")]')
        
        results_found = 0
        
        for result in results:
            identifier = result.css('h2.title::text, span.refNO::text').get()
            if not identifier:
                identifier = result.re_first(self.IDENTIFIER_PATTERNS)
            
            if identifier:
                item = WorkplaceDecisionItem()
                item['identifier'] = identifier.strip()
                results_found += 1
                
                desc = result.css('p.description::text').get()
                item['description'] = desc.strip() if desc else ''
                
                ref = result.css('span.refNO::text').get()
                item['ref_no'] = ref.strip() if ref else identifier.strip()
                
                date = result.css('span.date::text').get()
                item['published_date'] = date.strip() if date else None
                
                link = result.css('a.btn.btn-primary::attr(href), h2.title a::attr(href)').get()
                if link:
                    item['link_to_doc'] = response.urljoin(link)
                elif item['published_date']:
                    try:
                        day, month, year = item['published_date'].split('/')
                        month_name = datetime(int(year), int(month), 1).strftime('%B').lower()
                        item['link_to_doc'] = f"https://www.workplacerelations.ie/en/cases/{year}/{month_name}/{identifier.lower()}.html"
                    except Exception as e:
                        self.logger.warning(f"failed to construct link for {identifier}: {e}")
                        item['link_to_doc'] = None
                else:
                    item['link_to_doc'] = None
                
                item['partition_date'] = partition_label
                item['scraped_at'] = datetime.now().isoformat()
                
                yield item
        
        self.logger.info(f"{results_found} results")
        
        if results_found == 0:
            self.logger.warning(f"no results: {partition_label}")
            filename = DEBUG_DIR / f"response_{partition_label}_page{page}.html"
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.logger.warning(f"saved debug: {filename}")
        
        next_page = response.css('a.next::attr(href), a[rel="next"]::attr(href)').get()
        
        if next_page and results_found > 0:
            self.logger.info(f"page {page + 1}")
            yield response.follow(
                next_page,
                callback=self.parse,
                meta={
                    'partition_label': partition_label,
                    'page': page + 1
                }
            )
