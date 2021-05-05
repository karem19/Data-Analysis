import requests 
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
import pickle
from threading import  Thread
import os 
import pytz
from datetime import datetime
from typing import Optional
import pandas as pd 


items_list= []
base_url= 'https://ev.caltech.edu/api/v1/'
token= 'w0xq5Cd-Wtae7vw3cPgsHC2lH9xjx0SCyF6SR8EJbjI'


def parse_http_date(ds, tz):
    """ Convert a string in RFC 1123 format to a datetime object
    :param str ds: string representing a datetime in RFC 1123 format.
    :param pytz.timezone tz: timezone to convert the string to as a pytz object.
    :return: datetime object.
    :rtype: datetime
    """
    dt = pytz.UTC.localize(datetime.strptime(ds, "%a, %d %b %Y %H:%M:%S GMT"))
    return dt.astimezone(tz)

def parse_dates(doc):
    """ Convert all datetime fields in RFC 1123 format in doc to datetime objects.
    :param dict doc: document to be converted as a dictionary.
    :return: doc with all RFC 1123 datetime fields replaced by datetime objects.
    :rtype: dict
    """
    tz = pytz.timezone(doc["timezone"])
    for field in doc:
        if isinstance(doc[field], str):
            try:
                dt = parse_http_date(doc[field], tz)
                doc[field] = dt
            except ValueError:
                pass
        if isinstance(doc[field], dict) and "timestamps" in doc[field]:
            doc[field]["timestamps"] = [
                parse_http_date(ds, tz) for ds in doc[field]["timestamps"]
            ]


def events_generator(page_number): 
	"""Gets all the session from a given page number using the ACN-data API
	:param: int the number of the page to get access to  
	"""
    if(page_number==0): 
        r = requests.get(base_url+'sessions/caltech/ts?pretty', auth=HTTPBasicAuth(token, ""))
    else: 
        r = requests.get(base_url+f'sessions/caltech/ts?pretty&page={page_number}', auth=HTTPBasicAuth(token, ""))

    payload = r.json()

    #print(base_url+f'&page={page_number}')

    while True:
        for s in payload["_items"]:
            parse_dates(s)
            yield s
        if "next" in payload["_links"]:
            #print(payload["_links"]["next"]["href"])
            r = requests.get(
                base_url + payload["_links"]["next"]["href"], auth=(token, "")
                )
            payload = r.json()
        else:
            break



def get1000_elements(start):
	"""Gets 1000 sessions from a given starting position and saves their timeseries 
	in a csv file (Charging Current under /created/ChargingCurrent, Pilot Signal under created/PilotSignal/)
	:param: int starting session 
	"""
    sessions= events_generator(int(start/25))
    #ids= []
    for i in tqdm(range(1000)): 
        #try: 
        sess= next(sessions)
        # if(sess["_id"] in ids): 
        #     print('fucking error! ')
        #     print(sess["_id"],i)
        #     break
        #ids.append(sess["_id"])
        pd.DataFrame(sess['chargingCurrent']).to_csv(f'created/ChargingCurrent/{sess["_id"]}.csv')
        pd.DataFrame(sess['pilotSignal']).to_csv(f'created/PilotSignal/{sess["_id"]}.csv')

        #except: 
         #   break


def caller(start): 
    global items_list
    for page_num in tqdm(range(start+1,min(start+101,1158))) :
        response= requests.get("https://ev.caltech.edu/api/v1/sessions/caltech/ts?pretty&page"+str(page_num),auth = HTTPBasicAuth('w0xq5Cd-Wtae7vw3cPgsHC2lH9xjx0SCyF6SR8EJbjI', '')) 
        items_list= items_list + response.json()['_items']

        if(page_num%100 ==0): 
            open_file = open('data_sample'+str(page_num)+'.pkl', "wb")
            pickle.dump(items_list, open_file)
            open_file.close()


def one_page(n): 
    global items_list
    response= requests.get("https://ev.caltech.edu/api/v1/sessions/caltech/ts?pretty&page"+str(n),auth = HTTPBasicAuth(token, '')) 

    items_list= items_list + response.json()['_items']

    
start= int(input('Please enter a starting number '))
get1000_elements(start)

