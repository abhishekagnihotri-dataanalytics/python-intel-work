__author__ = "Abhishek Agnihotri"
__email__ = "abhishek.agnihotri@intel.com"
__description__ = "This script loads the GDACS data from the site into the SQL table"
__schedule__ = "6:30 PM PST"

from datetime import datetime
# from time import time
import requests
import urllib
import os
import sys;sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import http.client
import pandas as pd
from bs4 import BeautifulSoup
from Helper_Functions import uploadDFtoSQL,map_columns
from Logging import log
from dateutil import parser as date_parser
from Project_params import params


params['EMAIL_ERROR_RECEIVER'].append('abhishek.agnihotri@intel.com')

# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass



response= requests.get('https://www.gdacs.org/xml/rss.xml',proxies={'https': 'http://proxy-dmz.intel.com:912'})

data=response.content
xml_str=data.decode("utf-8")
xml_str
#
# conn = http.client.HTTPSConnection("www.gdacs.org")
#
# payload = ""
#
# headers = {
#     # 'cookie': "jrc_cookie=!%2FbG8snmYkAGkJQvNnV%2FSe0VbsbxmbPzgmxRT6TiiK87UH3YeXdvONner5T7FSfT5NVeRsVL068hfPT0%3D; TS017bc0d6=01f4fc03dd395ab6ab33e799e944bfe744ae349de78ac8ae927a40027bda81b152c396da1a12f6143532c40e8caefe9c0d1a0a4efe",
#     # 'Referer': "",
#     # 'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36"
#
# }
#
# conn.request("GET", "/xml/rss.xml", payload, headers)
#
# res = conn.getresponse()
# data = res.read()
#
# xml_str = data.decode("utf-8")


tags = ['title', 'description', 'temporary', 'link', 'pubDate', 'dateadded', 'datemodified', 'iscurrent', 'fromdate',
        'todate',
        'durationinweek', 'year', 'subject', 'guid', 'lat', 'long', 'bbox', 'point', 'cap', 'icon', 'version',
        'eventtype',
        'alertlevel', 'alertscore', 'episodealertlevel', 'episodealertscore', 'eventname', 'eventid', 'episodeid',
        'calculationtype',
        'severity', 'population', 'vulnerability', 'iso3', 'country']

soup = BeautifulSoup(data, 'xml')

split = []
for item in soup.find_all('item'):
    split.append(item)

data_ = []
for split_item in split:
    for tag in tags:
        data_.append(split_item.find(tag).get_text())

def split_list (big_list,x):
    target=[]
    for i in range(0,len(big_list), x):
         target.append(big_list[i:i+x])
    return target

listed_data=split_list (data_,35)

df=pd.DataFrame(listed_data)

columns_in_prod = ['Title', 'Description', 'Temporary', 'Link', 'PubDate', 'DateAdded', 'DateModified', 'IsCurrent', 'FromDate',
        'ToDate',
        'DurationInWeek', 'Year', 'Subject', 'Guid', 'Lat', 'Long', 'BBox', 'Point', 'Cap', 'Icon', 'Version',
        'EventType',
        'AlertLevel', 'AlertScore', 'EpisodeAlertLevel', 'EpisodeAlertScore', 'EventName', 'EventId', 'EpisodeId',
        'CalculationType',
        'Severity', 'Population', 'Vulnerability', 'Iso3', 'Country']
#
columns = ['Title', 'Description', 'Temporary', 'Link', 'PubDate', 'DateAdded', 'DateModified', 'IsCurrent', 'FromDate',
        'ToDate',
        'DurationInWeek', 'Year', 'Subject', 'Guid', 'Lat', 'Long', 'BBox', 'Point', 'Cap', 'Icon', 'Version',
        'EventType',
        'AlertLevel', 'AlertScore', 'EpisodeAlertLevel', 'EpisodeAlertScore', 'EventName', 'EventId', 'EpisodeId',
        'CalculationType',
        'Severity', 'Population', 'Vulnerability', 'Iso3', 'Country','LoadDtm','LoadBy']

df.columns=columns_in_prod
# df['UploadDate'] = datetime.today()


typechange_date=['PubDate', 'DateAdded', 'DateModified', 'FromDate', 'ToDate']

# numeric_cols=['durationinweek','year','version','alertscore','eventid','episodeid']

for columnname in typechange_date:
    df[columnname]=df[columnname].apply(lambda x: date_parser.parse(x))


df['LoadDtm'] = datetime.today()
df['LoadBy'] = 'AMR\\' + os.getlogin().upper()


output=map_columns('[dbo].[GDACS_Global_disaster_data]',df,columns)



project = 'GDACS_GlobalDisasterData'
driver = "{ODBC Driver 17 for SQL Server}"
success_bool, error_msg = uploadDFtoSQL('[dbo].[GDACS_Global_disaster_data]', df, columns, truncate=False, driver=driver)
log(success_bool, project_name=project, data_area='[dbo].[GDACS_Global_disaster_data]', row_count=df.shape[0], error_msg=error_msg)
