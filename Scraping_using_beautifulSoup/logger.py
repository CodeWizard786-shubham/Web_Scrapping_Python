# logging Implementation
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s -%(filename)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/home/hdoop/Documents/python/WebScrapingProject/WebScrapingProject/Scraping_using_beautifulSoup/beautifulSoup_implementation.log"),
        logging.StreamHandler(sys.stdout)
    ]
)


logger = logging.getLogger(__name__)


