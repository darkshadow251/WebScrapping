
from create_database import csv_to_database,find_top_25,top25_for_last30,openold_closelatest
from File_Functions import latest_bhav_file,recent_bhav_30,download_file

url='https://www.nseindia.com/market-data/securities-available-for-trading'

download_file(url,"Securities available for Equity segment (.csv)")  

latest_bhav_file()
recent_bhav_30()
csv_to_database()
output=find_top_25()
print(output)

f_output=top25_for_last30()
print(f_output)
output=openold_closelatest()
print(output)






