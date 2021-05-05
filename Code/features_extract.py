import pandas as pd 
import numpy as np 
from matplotlib import pyplot as plt
import seaborn as sns                                      
import json 
from datetime import datetime
import os 
import pickle 
from tqdm import tqdm 
import json 

#Import sessions data
data = json.load(open('acndata_sessions.json'))
sessions = pd.DataFrame(data['_items'])

#Get series with missing values lists
with open('current_error_list.pkl', 'rb') as f:
    current_nans = pickle.load(f)
with open('pilot_error_list.pkl', 'rb') as f:
    pilot_nans = pickle.load(f)

features_dict= {}
def time_to_sec(time): 
    """Converts datetime to seconds 
    :variables: datetime : time to convert 
    :returns: int: the converted time to seconds  
    """
    return (time.hour * 60 + time.minute) * 60 + time.second

def feature_extract(id): 
    """Extracts features like length/mean/max current from a given time series timeseries, aggretad wasted energy
    Input: a session ID (str)
    Output: A dictionary of different session features 
    """
    current_df= pd.read_csv(f'ChargingCurrent/{id}',parse_dates=["timestamps"]) 
    pilot_df= pd.read_csv(f'PilotSignal/{id}',parse_dates=["timestamps"])
    duration= time_to_sec(current_df['timestamps'][current_df.shape[0]-1])-time_to_sec(current_df['timestamps'][0])
    avg_current= current_df['current'].mean()
    max_current= current_df['current'].max()
    l = min(pilot_df.shape[0],current_df.shape[0])
    wasted= (np.array(pilot_df['pilot'])[:l]-np.array(current_df['current'])[:l]).sum() * duration 
    return {'Session duration (s)':duration, 'avg current': avg_current,'max current': max_current, 'wasted energy':wasted}



for session in tqdm(lost_sessions):
    try:
     
        features_dict[session.split('.')[0]]= feature_extract(session)
    except: 
        features_dict[session.split('.')[0]]= {}
            
      
    with open('missed_features.txt', 'w') as outfile:
        json.dump(features_dict, outfile)

with open('missed_features.txt', 'w') as outfile:
        json.dump(features_dict, outfile)


print('done')