import pandas as pd 
import numpy as np                                   
import json 
from datetime import datetime
import pickle
from tqdm import tqdm 
import  os 

def time_to_sec(time): 
    return (time.hour * 60 + time.minute) * 60 + time.second

pilot_error_list= []

for filename in tqdm(os.listdir('PilotSignal/')):
    try: 

        timeseries= pd.read_csv(f'PilotSignal/{filename}',usecols=['timestamps','pilot'],parse_dates=["timestamps"])
        t0= timeseries['timestamps'][0]
        t0= time_to_sec(t0)
        timeseries['Relative time']= timeseries['timestamps'].apply(lambda x: time_to_sec(x)-t0)
        timeseries.to_csv(f'PilotSignal/{filename}', index=False)

    except: 
        print(f'Error in {filename}')
        file1 = open("pilot_error.txt","a")#append mode
        file1.write(f'{filename}\n')
        file1.close()
        pilot_error_list.append(filename)


with open('pilot_error_list.pkl', 'wb') as f:
    pickle.dump(pilot_error_list, f)
