'''
@Author: Shubham Shirke
@Date: 2023-08-27 11:30:30
@Last Modified by: Shubham shirke
@Last Modified time: 2023-08-25 12:30:30
@Title : Proprocess and analysis hadoop stored records.
'''

# Data Preprocessing and Analysis
# Dataset to process - shiksha_records.csv
# Implement Logging
import logging
import sys
import os
logging.basicConfig(level=logging.INFO,format=" %(asctime)s - [%(levelname)s]-%(message)s" \
                    ,handlers=[logging.FileHandler("/home/hdoop/Documents/python/WebScrapingProject/WebScrapingProject/Scraping_using_beautifulSoup/shiksha_records_preprocessing.log"),
                               logging.StreamHandler(sys.stdout)])

logger = logging.getLogger(__name__)


# 1. Data Extraction
# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# create Spark object
logger.info("Data Preprocessing and Analysis Started....")
spark = SparkSession.builder.appName("Shiksha_Records_Preprocess").config("spark.driver.bindAddress","10.0.2.15").getOrCreate()
logger.info("Spark object Created")

# Read csv file from Hadoop hdfs

try:
    df = spark.read.csv("hdfs://localhost:9000/web_scraped_data/shiksha_records.csv",inferSchema=True,header=True)
    logger.info(df.show(10))
except Exception as e:
    logger.error("csv from hadoop succssfully loaded")
    
# 2. Data Exploration
# getting rows and columns count
df_rows_count = df.count()
df_columns_count = len(df.columns)
logger.info(f"Total Rows count: {df_rows_count}")
logger.info(f"Total Columns count: {df_columns_count}")


# 4.Data Transfromations
df = df.withColumnRenamed("Sr No", "sr_no") \
       .withColumnRenamed("College Name", "college_name") \
       .withColumnRenamed("Location", "location") \
       .withColumnRenamed("College Type", "college_type") \
       .withColumnRenamed("Number of courses", "number_of_courses") \
       .withColumnRenamed("Ratings", "ratings") \
       .withColumnRenamed("Exams Accepted", "exams_accepted") \
       .withColumnRenamed("Total Fee Range", "total_fee_range") \
       .withColumnRenamed("Average Package", "average_package")


df = df.withColumn("number_of_courses",split(col("number_of_courses"), " ").getItem(0))
df = df.withColumn("number_of_courses",col("number_of_courses").cast("int"))
df = df.withColumn("ratings",col("ratings").cast("float"))

# 5. Data Analysis
plot_df = df.toPandas()
plot_df.set_index(plot_df.columns[0], inplace=True)
plot_df
ratings_vs_type_df = plot_df.dropna(subset=["ratings"])

import plotly.express as px
import os

# Filter out rows with None values in the 'ratings' column
ratings_vs_type_df_filtered = ratings_vs_type_df.dropna(subset=["ratings"])

# Group by 'college_type' and calculate the mean ratings
new_ratings = ratings_vs_type_df_filtered.groupby('college_type')["ratings"].mean().reset_index()

# Creating an interactive bar plot using Plotly Express
fig = px.bar(new_ratings, x='college_type', y='ratings',
             labels={"college_type": "College Type", "ratings": "Ratings"},
             title="Ratings by College Type", color="college_type",
             color_discrete_sequence=["brown", "green", "blue"])

fig.update_layout(width=600, height=400)

# Store the plot image
plot_store_path = "/home/hdoop/Documents/python/WebScrapingProject/WebScrapingProject/Scraping_using_beautifulSoup/"
if os.path.exists(plot_store_path):
    fig.write_image(plot_store_path + "type_vs_ratings.png", format="png")


# plot for the number of courses

# Group by 'college_type' and calculate the mean number of courses
new_courses_df = ratings_vs_type_df_filtered.groupby('college_type')["number_of_courses"].mean().reset_index()

# Creating an interactive bar plot for number of courses using Plotly Express
fig = px.bar(new_courses_df, x='college_type', y='number_of_courses',
             labels={"college_type": "College Type", "number_of_courses": "Courses (count)"},
             title="Number of Courses by College Type")

fig.update_layout(width=600, height=400, barmode='group', bargap=0.5)

# Store the plot image
if os.path.exists(plot_store_path):
    fig.write_image(plot_store_path + "type_vs_number_of_courses.png", format="png")

logger.info("Exceution Finished")
spark.stop()