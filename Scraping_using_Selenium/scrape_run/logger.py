# logging Implementation
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s -%(filename)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/home/hdoop/Documents/python/WebScrapingProject/WebScrapingProject/Scraping_using_Selenium/run/selenium_implementation.log"),
        logging.StreamHandler(sys.stdout)
    ]
)


logger = logging.getLogger(__name__)


