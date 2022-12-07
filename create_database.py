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
from File_Functions import oldest_latest



def find_top_25():
    output=[]
    conn=msql.connect(host='localhost', database='NSE', user='root', password='')
    cursor=conn.cursor()
    st='''select symbol from NSE.bhav_data order by (close-open)/open desc limit 25'''
    cursor.execute(st)
    result = cursor.fetchall()
    for i in result:
        output.append(i)
    return output

def csv_to_database():
    filep=''
    directory=os.getcwd()
    for f in os.listdir(directory):
        print(f)
        if f.endswith('bhav.csv'):
            filep=f
    if filep:
        bhav_data=pd.read_csv(filep)
        del bhav_data["Unnamed: 13"]
        bhav_data=bhav_data.dropna()
       
        print(bhav_data.head)
        try:
            conn = msql.connect(host='localhost', user='root',  
                                password='')#give ur username, password
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("CREATE DATABASE NSE")
                print("Database is created")
        except Error as e:
            print("Error while connecting to MySQL", e)
        try:
            conn = msql.connect(host='localhost', database='NSE', user='root', password='')
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()
                print("You're connected to database: ", record)
                cursor.execute('DROP TABLE IF EXISTS bhav_data;')
                print('Creating table....')
        # in the below line please pass the create table statement which you want #to create
                cursor.execute("CREATE TABLE bhav_data(symbol varchar(25),series varchar(5),OPEN float(15,3),high float(15,3),low float(15,3),close float(15,3),last float(15,3),prevclose float(15,3),TOTTRDQTY int,TOTTRDVAL float(20,3),TIMESTAMP varchar(25),TOTALTRADES int, ISIN varchar(25))")
                print("Table is created....")
                #loop through the data frame
                for i,row in bhav_data.iterrows():
                    #here %S means string values 
                    sql = "INSERT INTO NSE.bhav_data VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql, tuple(row))
                    print(i,"Record inserted")
                    # the connection is not auto committed by default, so we must commit to save our changes
                    conn.commit()
        except Error as e:
            print("Error while connecting to MySQL", e)

def top25_for_last30():
    f_output={}
    directory=os.getcwd()
    path=directory+'\\'+'bhav_last_30'
    directory="C:/Users/omdod/Desktop/Internship_Assignment/bhav_last_30"
    for f in os.listdir(path):

        if f.endswith('bhav.csv'):
            date=f[2:11]
            print(date)
            f_output[date]=top_gainers_of_day(path+'\\'+f)
    return f_output

def top_gainers_of_day(filep):
    output=[]
    if filep:
        bhav_data=pd.read_csv(filep)
        del bhav_data["Unnamed: 13"]
        bhav_data=bhav_data.dropna()
       
        print(bhav_data.head)
        try:
            conn = msql.connect(host='localhost', user='root',  
                                password='')#give ur username, password
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("CREATE DATABASE NSE")
                print("Database is created")
        except Error as e:
            print("Error while connecting to MySQL", e)
        try:
            conn = msql.connect(host='localhost', database='NSE', user='root', password='')
            if conn.is_connected():
                cursor = conn.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()
                print("You're connected to database: ", record)
                cursor.execute('DROP TABLE IF EXISTS bhav_data;')
                print('Creating table....')
        # in the below line please pass the create table statement which you want #to create
                cursor.execute("CREATE TABLE bhav_data(symbol varchar(25),series varchar(5),OPEN float(15,3),high float(15,3),low float(15,3),close float(15,3),last float(15,3),prevclose float(15,3),TOTTRDQTY int,TOTTRDVAL float(20,3),TIMESTAMP varchar(25),TOTALTRADES int, ISIN varchar(25))")
                print("Table is created....")
                #loop through the data frame
                for i,row in bhav_data.iterrows():
                    #here %S means string values 
                    sql = "INSERT INTO NSE.bhav_data VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql, tuple(row))
#                     print(i,"Record inserted")
                    # the connection is not auto committed by default, so we must commit to save our changes
                    conn.commit()
                st='''select symbol from bhav_data order by (close-open)/open desc limit 25'''
                cursor.execute(st)
                result = cursor.fetchall()
                for i in result:
                    output.append(i)
                return output
        except Error as e:
            print("Error while connecting to MySQL", e)

def openold_closelatest():
    oldest,latest=oldest_latest()
    print(oldest,latest)
    oldest=os.getcwd()+'\\'+'bhav_last_30'+'\\'+oldest
    latest=os.getcwd()+'\\'+'bhav_last_30'+'\\'+latest
    df1=pd.read_csv(oldest)
    df2=pd.read_csv(latest)
    df1=df1.drop(['SERIES','HIGH','LOW','CLOSE','LAST','PREVCLOSE','TOTTRDQTY','TOTTRDVAL','TIMESTAMP','TOTALTRADES','ISIN','Unnamed: 13'],axis=1)
    df2=df2.drop(['SERIES','OPEN','HIGH','LOW','LAST','PREVCLOSE','TOTTRDQTY','TOTTRDVAL','TIMESTAMP','TOTALTRADES','ISIN','Unnamed: 13'],axis=1)
   
    try:
        conn = msql.connect(host='localhost', user='root',  password='')#give ur username, password
        
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE NSE")           
            print("Database is created")
        
    except Error as e:
        print("Error while connecting to MySQL", e)
    
    try:
        
        output=[]
        conn = msql.connect(host='localhost', database='NSE', user='root', password='')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
            cursor.execute('DROP TABLE IF EXISTS latest_bhav_data;')
            cursor.execute('DROP TABLE IF EXISTS oldest_bhav_data;')
            print('Creating table....')
                # in the below line please pass the create table statement which you want #to create
            cursor.execute("CREATE TABLE latest_bhav_data(symbol varchar(25),close float(10,3))")
            cursor.execute("CREATE TABLE oldest_bhav_data(symbol varchar(25),open float(10,3))")
                        #loop through the data frame
            for i,row in df2.iterrows():
                                #here %S means string values 
                sql = "INSERT INTO NSE.latest_bhav_data VALUES (%s,%s)"
                cursor.execute(sql, tuple(row))
                print(i,"Record inserted")
                            # the connection is not auto committed by default, so we must commit to save our changes
                conn.commit()
            for i,row in df1.iterrows():
                            #here %S means string values 
                sql = "INSERT INTO NSE.oldest_bhav_data VALUES (%s,%s)"
                cursor.execute(sql, tuple(row))
                print(i,"Record inserted")
                conn.commit()
            st='''select l.symbol from latest_bhav_data as l inner join oldest_bhav_data as o on l.symbol=o.symbol order by (l.close-o.open)/o.open desc limit 25'''
            cursor.execute(st)
            result = cursor.fetchall()
            for i in result:
                output.append(i)
            return output
        
    except Error as e:
        print("Error while connecting to MySQL", e)

