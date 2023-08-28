'''
@Author: Shubham Shirke
@Date: 2023-07-26 11:30:30
@Last Modified by: Shubham shirke
@Last Modified time: 2023-08-25 17:30:30
@Title : Scrape Records from given website using bs4 library,Process records using pyspark 
        and store the records in hdfs.
        Note: run main.py file to start scraping
'''

from logger import logger
from bs4_scraping import scrape_records
from store_records_in_hadoop import store_in_hadoop


#Driver Code
def main():
    logger.info("Scraping using Beautiful Soup Started...")
    scrape_records()
    logger.info("Scraping finished")
    store_in_hadoop()
    
# Execution starts
if __name__ == "__main__":
    main()