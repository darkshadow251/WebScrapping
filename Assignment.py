
from create_database import csv_to_database,find_top_25,top25_for_last30,openold_closelatest
from File_Functions import latest_bhav_file,recent_bhav_30,download_file

url='https://www.nseindia.com/market-data/securities-available-for-trading'

download_file(url,"Securities available for Equity segment (.csv)")  # to download the securities files

latest_bhav_file() # to download latest bhav file
recent_bhav_30()# to download bhav files from last 30 days
csv_to_database()# to convert csv to database
output=find_top_25()# to find top 25 gainers 
print(output)

f_output=top25_for_last30()# to find top 25 gainers for last 30 days for each day
print(f_output)
output=openold_closelatest()# to find the top gainers using close of latest bhav file and open from oldest bhav file in last month
print(output)






