'''
@Author: Shubham Shirke
@Date: 2023-07-21 11:30:30
@Last Modified by: Shubham shirke
@Last Modified time: 2023-08-25 17:30:30
@Title : Scrape Records from given website using selenium library,Process records using pyspark 
        and store the records in hdfs.
        Note: run main.py file to perform start scraping
'''

# Define Libraries
from logger import logger
from scrape_records import scrape_data
from store_in_hadoop import store_in_hadoop


# Driver Code
def main():
    try:
        logger.info("Selenium Scrapping started...")
        total_elapsed_time = scrape_data()
        logger.info(f"Total scrape Time taken:{total_elapsed_time}")
        store_in_hadoop()
        logger.info("Scraping Finished ...")

    except Exception as e:
        logger.error(str(e))



# Exceution starts
if __name__ == "__main__":
    main()