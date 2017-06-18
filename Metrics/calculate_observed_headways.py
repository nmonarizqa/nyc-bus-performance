import pandas as pd
from datetime import datetime, timedelta
import datetime as dt
import requests
import zipfile as zf
import StringIO
import re
import os
import numpy as np
import sys
from os.path import join, isfile, abspath
from os import listdir

## init
today_str = '2017-05-01'

## paths
calls_path = "./data/calls")
trips_path = "./data/trips"
trips = pd.read_csv(join(trips_path, "all_trips.csv"))
stops_path = "./data/stops")
stops = pd.read_csv(join(stops_path, "all_stops.csv"))

## read calls
def get_calls(today_str):
    # today
    today = dt.datetime.strptime(today_str,'%Y-%m-%d')
    filepath_today = calls_path + "/" + today_str[:7] + "/" + today_str + ".csv"
    df0 = pd.read_csv(filepath_today)
    # tomorrow
    tomorrow = today + timedelta(days=1)
    tomorrow_str = dt.datetime.strftime(tomorrow,'%Y-%m-%d')
    filepath_tomorow = calls_path + "/" + tomorrow_str[:7] + "/" + tomorrow_str + ".csv"
    df1 = pd.read_csv(filepath_tomorrow)
    # merge both
    calls = pd.concat([df0, df1]).sort_values(['vehicle_id','timestamp'])
    calls = calls[calls.service_date == int(today_str)]
    return calls

## get at stop buses
def get_at_stop(calls):
    last_date = '0000-00-00'
    last_trip = -1
    last_vehicle = 0
    last_stop = -1
    last_time = dt.datetime.now()
    
    columns = ['vehicle_id','service_date','trip_id','next_stop_id','timestamp']
    init = pd.DataFrame(pd.Series([last_vehicle, last_date, last_trip, last_stop, last_time], columns)).T
    block_assigned = calls.block_assigned
    calls = calls[columns]
    calls = init.append(calls)
    calls.index = range(len(calls))
    
    # add calculation columns
    calls['next_next_stop_id'] = calls.next_stop_id.tolist()[1:] + [-1]
    calls['next_time'] = calls.timestamp.tolist()[1:] + [np.nan]
    calls['comp'] = calls.next_stop_id == calls.next_next_stop_id
    
    # divided calls based on comparison status
    calls1 = calls[calls.comp==True].copy()
    calls2 = calls[calls.comp==False].copy()
    calls1['dep_start'] = calls1['next_time']
    calls2['dep_start'] = calls2['timestamp']
    calls1['dep_end'] = np.nan
    calls2['dep_end'] = calls2['next_time']
    
    # merge them all again
    calls = pd.concat([calls1,calls2])
    calls=calls.sort_index().iloc[:-1,:]
    calls['block_assigned'] = block_assigned.tolist()
    clean_calls = calls.dropna()
    return clean_calls

def get_necessary_columns(clean_calls, trips):
    # merge calls and trips
    merged = pd.merge(clean_calls, trips, how='left', on='trip_id')
    merged = merged[['service_date','trip_id','vehicle_id','next_stop_id',
                'dep_start','dep_end','route_id','direction_id','block_assigned']]
    merged = merged.dropna()
    
    # convert time to Eastern Time
    def convert_utc(x):
        return dt.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ") - timedelta(hours=5)
    merged['dep_start'] = merged['dep_start'].apply(convert_utc)
    merged['dep_end'] = merged['dep_end'].apply(convert_utc)
    
    # interpolating time to get expected departure time
    merged['timestamp'] = merged.dep_start +  (merged.dep_end-merged.dep_start)/2
    # sort
    merged = merged.sort_values(['next_stop_id','route_id','timestamp'])[merged.next_stop_id != '\N']
    
    # select necessary columns
    merged = merged[['trip_id','next_stop_id','route_id','direction_id','block_assigned','timestamp']]
    return merged

def calculate_headways(merged):
    # add calculation columns
    merged['next_route'] = merged.route_id.tolist()[1:] + [np.nan]
    merged['next_time'] = merged.timestamp.tolist()[1:] + [np.nan]
    merged['next_next_stop'] = merged.next_stop_id.tolist()[1:] + [np.nan]
    
    # compare
    merged['comp'] = merged.route_id == merged.next_route
    merged['comp2'] = merged.next_next_stop == merged.next_stop_id
    merged['comp3'] = merged['comp'] & merged['comp2']
    
    # remove last stops and unnecessary columns
    merged = merged[merged.comp3==True]
    merged.drop(['next_route','comp','next_next_stop','comp2','comp3'], axis=1, inplace=True)
    
    # calculate headways
    merged['headways'] = merged.next_time - merged.timestamp
    merged['headways'] = merged.headway.dt.total_seconds()/60
    merged['rds_index'] = merged.route_id.astype(str) + '_' + merged.direction_id.astype(int).astype(str) + '_' + merged.next_stop_id.astype(str)
    merged = merged[['rds_index','trip_id','timestamp','headways']]
    outname = 'observed_headways_'+ today_str +'.csv'
    merged.to_csv(outname), index=False)
    return outname
    
def main():
    print "Loading calls...\n"
    calls = get_calls(today_str)
    print "Done. \n Filtering at stop buses...\n"
    clean_calls = get_at_stop(calls)
    print "Done. \n Removing last stops... \n"
    merged = get_necessary_columns(clean_calls, trips)
    print "Done. \n Calculating headways... \n"
    outname = calculate_headways(merged)
    print "Done. \n Output stored as %s" %outname

main()
    

    
    
    