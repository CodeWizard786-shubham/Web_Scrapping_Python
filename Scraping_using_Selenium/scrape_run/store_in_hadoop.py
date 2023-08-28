from pyspark.sql import SparkSession
from logger import logger

def store_in_hadoop():
    try:
        logger.info("Loding data in hadoop started..")
        
        # create spark session
        spark = SparkSession.builder.master('local').appName("CollegeDuniaRecords").config("spark.driver.bindAddress","10.0.2.15").getOrCreate() 

        input_file_path = '/home/hdoop/Documents/python/WebScrapingProject/WebScrapingProject/Scraping_using_Selenium/csv_data_file/college_dunia.csv'
        
        # Read csv
        df = spark.read.format("csv").options(inferSchema=True, sep=",", header=True).load(input_file_path)
        
        # write csv file to hadoop filesystem
        df.write.format("csv").save("hdfs://localhost:9000/web_scraped_data/college_dunia_records.csv",header=True,inferschema=True)
        
        spark.stop()
        logger.info("scraped data Sucessfully Loaded in Hadoop")
        
    except Exception as e:
        logger.error(f"Loading data in Hadoop Failed:{str(e)}")
        