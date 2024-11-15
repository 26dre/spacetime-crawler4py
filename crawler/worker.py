from threading import Thread
import os,sys

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
from scraper import saveFile
import time
import globals

filename = 'data.txt'

def print_data():
    print(f"Total unique pages: {len(globals.unique_urls)}")

    # Print the longest page info
    print(f"Longest page URL: {globals.longest_page['url']}")
    print(f"Longest page word count: {globals.longest_page['word_count']}")

    # Print top 50 words
    sorted_words = sorted(globals.word_frequencies.items(),
                          key=lambda item: item[1], reverse=True)
    top_50_words = sorted_words[:50]
    print("Top 50 words:")
    for word, freq in top_50_words:
        print(f"{word}: {freq}")

    # Print subdomains
    sorted_subdomains = sorted(globals.subdomains.items())
    print("Subdomains:")
    for subdomain, count in sorted_subdomains:
        print(f"{subdomain}, {count}")
        
    print("Data Printed!")

        

class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        count = 0
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                print_data()
                saveFile()
                break
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            
            count+=1
            if count >= 100:
                print_data()
                saveFile()
                count = 0  
            time.sleep(self.config.time_delay)
            
            
if __name__ == "__main__":
    saveFile()