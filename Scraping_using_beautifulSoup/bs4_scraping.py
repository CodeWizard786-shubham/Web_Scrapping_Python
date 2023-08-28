from bs4 import BeautifulSoup
from logger import logger
import requests
from time import time
import csv


def url(pn_number):
    # get url and params
    try:
        base_url = 'https://www.shiksha.com/search'
        params = {
            'q': 'list%20of%20colleges',
            'pn': pn_number  # Start with page 1
        }

        return base_url,params
    except Exception as e:
        logger.error(str(e))
        
        
def connect_webpage(base_url,params):
    try:
        # connect chrome driver 
        headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'}
        page = requests.get(base_url,headers=headers,params=params)
        return page
    except Exception as e:
        logger.error(str(e))
        
        
# Function to handle missing values      
def extract_text(element):
    return element.text.strip() if element else 'NULL'


# main scraping function
def scrape_records():
    try:
        start_time=time()
        params={'pn':1}
        with open("/home/hdoop/Documents/python/WebScrapingProject/WebScrapingProject/Scraping_using_beautifulSoup/records/shiksha_records.csv",mode="a",encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile,delimiter=",")
            writer.writerow(["Sr No","College Name","Location","College Type","Number of courses","Ratings","Exams Accepted",'Total Fee Range','Average Package'])
            count=0
            last_page_number=809
            page_number=1
            record = 0
            params['pn']= 1
            while params['pn'] <= last_page_number:
                base_url, params = url(params['pn'])  # Get the updated URL and parameters
                page = connect_webpage(base_url, params)
                # create soup object to parse though website html
                soup = BeautifulSoup(page.content,'html.parser')
                results = soup.find(class_ = "ctpSrp-contnr")
                for elements in results: # only if elemtns are present in results
                    college_name_elements = elements.find_all_next("div",class_="c43a")
                    location = elements.find_all_next("div",class_ = "edfa")
                    content_columns = elements.find_all_next("div", class_="cd4f _5c64 contentColumn_2")
                    if college_name_elements:
                        for college, loc,columns in zip(college_name_elements, location,content_columns):
                            count +=1
                            college_name = extract_text(college.find_next("h3"))
                            location = extract_text(loc.find_next("span",class_="_5588"))  # Get the second last span tag for location
                            college_type = extract_text(loc.find_all_next("span")[2])
                            number_of_courses = extract_text(columns.find_next("a" ,class_="_9865 ripple dark"))
                            ratings = extract_text(columns.find_next("span"))
                            exams_accespted = extract_text(columns.find_next("ul",class_="_0954"))
                            total_fee_range = extract_text(columns.find_all_next("div",class_ ="dcfd undefined")[2])
                            average_package = extract_text(columns.find_next("a", class_="ripple dark"))
                            writer.writerow([count,college_name, location,college_type,number_of_courses,ratings,exams_accespted,total_fee_range,average_package])
                            record +=1
                
                params['pn'] += 1  # Move to the next page
                page_number +=1
                if params['pn'] > last_page_number:
                    print("College records Successfully scraped")
                    stop_time=time()
                    total_scrape_time = (stop_time-start_time) /60
                    logger.info(f"Total time taken for scraping records:{total_scrape_time}")
                    break

    except Exception as e:
        logger.error(e)

    finally:
        with open("/home/hdoop/Documents/python/WebScrapingProject/WebScrapingProject/Scraping_using_beautifulSoup/records/page_number.csv",mode='w',encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(['Page_Number','Record'])
            writer.writerow([page_number,record])
            