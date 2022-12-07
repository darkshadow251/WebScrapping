import requests
from urllib.request import urlretrieve,urlopen
import requests
import mysql.connector as msql
from mysql.connector import Error
import os
from datetime import datetime,timedelta
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
from selenium.webdriver import Chrome,ChromeOptions
from selenium.webdriver.common.by import By
import requests
import time
from zipfile import ZipFile


def url_checker(url):
	try:
		get = requests.get(url)
		if get.status_code == 200:
			return(True)
		else:
			return(False)
	except requests.exceptions.RequestException as e:
		return(False)

    


def latest_bhav_file():
    link=""
    chrome_options = ChromeOptions()
    # chrome_options.add_argument("--start-maximized")
    prefs = {"download.default_directory": r"C:\Users\omdod\Desktop\Internship_Assignment\\"}
    chrome_options.add_experimental_option("prefs", prefs)


    driver=Chrome(executable_path="C:/Users/omdod/Downloads/chromedriver_win32/chromedriver", options=chrome_options)
    
    str="https://www1.nseindia.com/content/historical/EQUITIES/2022/"
    today=datetime.now()

    for i in range(30):
        day=today-timedelta(days=i)
        date=day.strftime('%d')
        month=day.strftime('%h')
        year=day.strftime('%Y')
        out=str+month.upper()+"/cm"+date+month.upper()+year+"bhav.csv.zip"
        try:
            driver.get(out)
        except Exception as e:
            print(e)
            
        if (url_checker(out)):
            link=out
            break
    if(link):
        with ZipFile(link[link.rfind('/')+1:], 'r') as zObject:
            zObject.extractall()

def oldest_latest():
    today=datetime.now()
    directory=os.getcwd()
    path=directory+'\\'+'bhav_last_30'
    latest=""
    oldest=""
    for i in range(30):
        day=today-timedelta(days=i)
        date=day.strftime('%d')
        month=day.strftime('%h')
        year=day.strftime('%Y')

        out="cm"+date+month.upper()+year+"bhav.csv"
        if latest=="" and out in os.listdir(path):
            latest=out
        if out in os.listdir(path):
            oldest=out
    return oldest,latest

def download_file(url,filename=''):
    driver=Chrome(executable_path="C:/Users/omdod/Downloads/chromedriver_win32/chromedriver")
    driver.get(url)

    time.sleep(3)
    element=driver.find_elements(By.CLASS_NAME,"pdf-download-link")
    for elements in element:
        if elements.text==filename:
            urlretrieve(elements.get_attribute('href'),'Equity_Segment.csv')
            break
    driver.quit()
        

def recent_bhav_30():
    chrome_options = ChromeOptions()
    os.mkdir("bhav_last_30")
    # chrome_options.add_argument("--start-maximized")
    prefs = {"download.default_directory": r"C:\Users\omdod\Desktop\Internship_Assignment\bhav_last_30\\"}
    chrome_options.add_experimental_option("prefs", prefs)

    
    driver=Chrome(executable_path="C:/Users/omdod/Downloads/chromedriver_win32/chromedriver", options=chrome_options)
    str="https://www1.nseindia.com/content/historical/EQUITIES/2022/"
    today=datetime.now()
   

    for i in range(30):
        day=today-timedelta(days=i)
        date=day.strftime('%d')
        month=day.strftime('%h')
        year=day.strftime('%Y')
        out=str+month.upper()+"/cm"+date+month.upper()+year+"bhav.csv.zip"
        try:
            driver.get(out)
        except Exception as e:
            print(e)
        print(out)
    
    directory=os.getcwd()
    for file in os.listdir(directory+"\\"+"bhav_last_30"):
        if file.endswith("bhav.csv.zip"):
            print('YES')
            with ZipFile(directory+"\\"+"bhav_last_30"+'\\'+file, 'r') as zObject:
                zObject.extractall(path=directory+'\\'+"bhav_last_30")

    for file in os.listdir(directory+'\\'+"bhav_last_30"):
        if file.endswith('bhav.csv.zip'):
            os.remove(directory+"\\"+"bhav_last_30"+'\\'+file)





