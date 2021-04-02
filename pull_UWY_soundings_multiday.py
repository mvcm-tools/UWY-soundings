# script to pull Radiosonde data from the Fairbanks Airport
# uses BeautifulSoup4 package to pull text from specified URL

import urllib
from bs4 import BeautifulSoup
import sys
import pandas as pd
import urllib.request
import datetime
import time

print('Usage: '+sys.argv[0]+' startdate enddate')
print(' ')
print('start and end dates in YYYY-MM-DD format')

# UWY sounding data base url 
baseurl = 'http://weather.uwyo.edu/cgi-bin/sounding?region=naconf&TYPE=TEXT%3ALIST'

# function to pull sounding data from url and return as a list
def sounding(sounding_date,sounding_time):
    year = '&YEAR='+str(sounding_date.year)
    month = str(sounding_date.month)
    day = str(sounding_date.day)
    station = '&STNM='+str(70261) #PAFA station name is 70261
    if len(str(month)) == 1:
        month = '&MONTH='+str(0)+str(month)
    else:
        month = '&MONTH='+str(month)
    if len(str(day)) == 1:
        fr = '&FROM='+str(0)+str(day)+sounding_time
        to = '&TO='+str(0)+str(day)+sounding_time
    else:
        fr = '&FROM='+str(day)+sounding_time
        to = '&TO='+str(day)+sounding_time
    url = str(baseurl+year+month+fr+to+station)
    html = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(html,features="html.parser")
    for script in soup(["script", "style"]):
        script.extract()
    text = soup.get_text()
    return(text)


# function to clean data and output a text file
def clean_sounding(data_in,date_to_get,sounding_time,cols):
    rawdata = data_in.split('\n')
    rawdata =  rawdata[9:]
    lines = []
    for xi in range(0, len(rawdata)):
        line = rawdata[xi].split(' ')
        line_new = []
        for xi in range(0,len(line)):
            if len(line[xi]) > 0:
                line_new.append(line[xi])
        if len(line_new) == 11:
            if line_new[0] != 'Questions':
                lines.append(line_new)
    df = pd.DataFrame(lines)
    df.columns = cols
    metadata_raw = rawdata[len(rawdata)-50:len(rawdata)-1]
    metadata = []
    for xi in range(0,len(metadata_raw)):
        metadata.append(metadata_raw[xi].split(' '))
    if sounding_time == 'noon':
        df.loc[:,'datetime_UTC'] = str(date_to_get+pd.Timedelta('12H'))
        df.index = df['datetime_UTC']
        df = df.drop(['datetime_UTC'],axis=1)
        return(df)
    if sounding_time == 'midnight':
        df.loc[:,'datetime_UTC'] = str(date_to_get)
        df.index = df['datetime_UTC']
        df = df.drop(['datetime_UTC'],axis=1)
        return(df)

# user input dates
st = str(sys.argv[1])
ed = str(sys.argv[2])
dates = pd.date_range(st,ed,freq='1D')

# make list to hold sounding data and column names
ls = []

for zi in range(0,len(dates)):
    day = dates[zi]
    # get soundings at 00Z and 12Z
    try:
        midnight = sounding(day,'00')
    except:
        print('HTTP Error 503: Service Unavailable')
        print(' ')
        print('waiting 30 sec.')
        time.sleep(30)
        try:
            midnight = sounding(day,'00')
        except:
            print('HTTP Error 503: Service Unavailable')
            print(' ')
            print('-- bailing, try again or extend wait time "time.sleep(sec)"')
            exit()
    try:
        noon = sounding(day,'12')
    except:
        print('HTTP Error 503: Service Unavailable')
        print(' ')
        print('waiting 30 sec.')
        time.sleep(30)
        try:
            noon = sounding(day,'12')
        except:
            print('HTTP Error 503: Service Unavailable')
            print(' ')
            print('-- bailing, try again or extend wait time "time.sleep(sec)"')
            exit()
 # extract column headers with units for midnight sounding if found
    unit_cols_mid = []
    mid_raw = midnight.split('\n')
    mid_check = mid_raw[1].split(' ')
# make empty list if sounding not found
    if mid_check[0] == "Can't":
        df_midnight = []
        print('Sounding for'+mid_check[9]+' '+mid_check[8]+' '+mid_check[10]+' at 00Z not found')
    else:
        cols_raw = mid_raw[6].split(' ')
        cols_new = []
        for xi in range(0,len(cols_raw)):
            if len(cols_raw[xi]) > 0:
                cols_new.append(cols_raw[xi])
        units = mid_raw[7].split(' ')
        units_new = []
        for xi in range(0,len(units)):
            if len(units[xi]) > 0:
                units_new.append(units[xi])
        for xi in range(0, len(cols_new)):
            if units_new[xi] == '%':
                unit_cols_mid.append(cols_new[xi]+'_pct')
            elif units_new[xi] == 'g/kg':
                unit_cols_mid.append(cols_new[xi]+'_g_kg')
            else:
                unit_cols_mid.append(cols_new[xi]+'_'+units_new[xi])
        df_midnight = clean_sounding(midnight,day,'midnight',unit_cols_mid)
    # extract column headers with units for noon sounding if found
    unit_cols_noon = []
    noon_raw = noon.split('\n')
    noon_check = noon_raw[1].split(' ')
    # make empty list if sounding not found
    if noon_check[0] == "Can't":
        df_noon = []
        print('Sounding for '+noon_check[9]+' '+noon_check[8]+' '+noon_check[10]+' at 12Z not found')
    else:
        cols_raw = noon_raw[6].split(' ')
        cols_new = []
        for xi in range(0,len(cols_raw)):
            if len(cols_raw[xi]) > 0:
                cols_new.append(cols_raw[xi])
        units = noon_raw[7].split(' ')
        units_new = []
        for xi in range(0,len(units)):
            if len(units[xi]) > 0:
                units_new.append(units[xi])
        for xi in range(0, len(cols_new)):
            if units_new[xi] == '%':
                unit_cols_noon.append(cols_new[xi]+'_pct')
            elif units_new[xi] == 'g/kg':
                unit_cols_noon.append(cols_new[xi]+'_g_kg')
            else:
                unit_cols_noon.append(cols_new[xi]+'_'+units_new[xi])   
        df_noon = clean_sounding(noon,day,'noon',unit_cols_noon)
    if len(df_noon) == 0:
        ls.append(df_midnight)
    elif len(df_midnight) == 0:
        ls.append(df_noon)
    else:
        df = pd.concat([df_midnight,df_noon])
        ls.append(df)



# concatenate list into dataframe for output
if len(ls) == len(dates):
    output = pd.concat(ls)
    output.to_csv(path_or_buf = 'PAFA-soundings-'+st[0:4]+st[5:7]+st[8:10]+'_'+ed[0:4]+ed[5:7]+ed[8:10]+'.txt', sep = '\t', na_rep = 'NaN', float_format='%.4f')
    print('wrote data to file: PAFA-soundings-'+st[0:4]+st[5:7]+st[8:10]+'_'+ed[0:4]+ed[5:7]+ed[8:10]+'.txt')
else:
    print('Failed to pull all dates from server, try again or extend wait time "time.sleep(sec)"')











    





