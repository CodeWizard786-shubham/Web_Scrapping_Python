from selenium import webdriver
from selenium.webdriver.common.by import By
from logger import logger
from time import time
import csv
import os

record_file_path = "/home/hdoop/Documents/python/WebScrapingProject/WebScrapingProject/Scraping_using_Selenium/csv_data_file/record_count.txt"

def get_driver():
    try:
        driver = webdriver.Chrome()
        return driver
    except Exception as e:
        logger.error(str(e))


#access webisite to scrape
def access_url():
    try:
        url = 'https://collegedunia.com/india-colleges?custom_params=%5Bview%3Atable%5D'
        driver=get_driver()
        driver.get(url)
        return driver
    except Exception as e:
        logger.error(str(e))

# get records count in terms of fetching failure at any point
def get_record_count():
    
    # Check if the record_count.txt file exists
    if os.path.exists(record_file_path):
        
        # If it exists, read the current record count from the file
        with open(record_file_path, 'r') as file:
            record_count = int(file.read())
        return record_count
    else:
        
        # If it doesn't exist, start from the beginning (record_count = 0)
        return 0


# update records counts after each record fetched
def update_record_count(count):   
    
    # Update the record count in the record_count.txt file
    with open(record_file_path, 'w') as file:
        file.write(str(count))

# main scrape function which will perfrom all the scraping task
def scrape_data():
    try:
        start_time = time()
        driver = access_url()
        table = driver.find_element(By.TAG_NAME, value="thead")
        entries = table.find_elements(By.TAG_NAME, "tr")

        headers = [th.text for th in entries[0].find_elements(By.TAG_NAME, "th")]
        
        # store fetched records directly in csv file
        with open('/home/hdoop/Documents/python/WebScrapingProject/WebScrapingProject/Scraping_using_Selenium/csv_data_file/college_dunia.csv', mode='a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Check the record count from the record_count.txt file
            record_count = get_record_count()
            if record_count == 0:
                writer.writerow(headers)
            stored_records = set()
            final_element = "jsx-2796823646.jsx-1933831621.endOfContainer"
            while final_element not in driver.page_source:
                driver.execute_script("arguments[0].scrollIntoView();", driver.find_element(By.CLASS_NAME, final_element))
                driver.implicitly_wait(2)

                # Using XPath to get all <tr> elements in the <tbody>
                entries = driver.find_elements(By.XPATH, "//tbody/tr")

                for entry in entries[record_count:]:
                    data_rows = [td.text.strip().replace("\n", " ") for td in entry.find_elements(By.XPATH, "./td")]
                    data_rows = [row for row in data_rows if row]
                    if data_rows: 
                        record = '\t'.join(data_rows)
                        if record not in stored_records: 
                            stored_records.add(record)
                            if '#' in data_rows[0]: 
                                writer.writerow(data_rows)
                        
                    record_count += 1

                # Update the record count in the record_count.txt file
                update_record_count(record_count)

                if final_element in driver.page_source:
                    break
        stop_time = time()            
        driver.close()
        logger.info("Data Scrapped in data.csv file")
        total_elapsed_time = (stop_time - start_time) / 60
        return total_elapsed_time
    except Exception as e:
        logger.error("Error occurred: ",str(e))

