import pandas as pd
from datetime import datetime, timedelta
import datetime as dt
import os
import numpy as np
import sys
from os.path import join, isfile, abspath
from os import listdir


## init
today_str = sys.argv[1]

## paths
calls_path = "./data/calls"
trips_path = "./data/trips"
trips = pd.read_csv(join(trips_path, "all_trips.csv"))
stops_path = "./data/stops"
stops = pd.read_csv(join(stops_path, "all_stops.csv"))

def download_calls(date_str):
    server = "http://data.mytransit.nyc/bus_time"
    year = date_str.split("-")[0]
    yearmonth = date_str[:7]
    if not os.path.exists(calls_path + '/' + yearmonth):
        os.mkdir(calls_path + '/' + yearmonth)                  
    path = "%s/%s/%s/bus_time_%s.csv.xz" %(server, year, yearmonth, date_str.replace("-",""))
    destination = "%s/%s/%s.csv.xz" %(calls_path, yearmonth, date_str)
    csvname = "%s/%s/%s.csv" %(calls_path, yearmonth, date_str)
    comm = "curl -o %s %s; unxz %s" %(destination, path, destination)
    os.system(comm)
    return pd.read_csv(csvname)

## read calls
def get_calls(today_str):
    # today
    today = dt.datetime.strptime(today_str,'%Y-%m-%d')
    filepath_today = calls_path + "/" + today_str[:7] + "/" + today_str + ".csv"
    try: 
        df0 = pd.read_csv(filepath_today)
    except:
        df0 = download_calls(today_str)
    # tomorrow
    tomorrow = today + timedelta(days=1)
    tomorrow_str = dt.datetime.strftime(tomorrow,'%Y-%m-%d')
    filepath_tomorrow = calls_path + "/" + tomorrow_str[:7] + "/" + tomorrow_str + ".csv"
    try: 
        df1 = pd.read_csv(filepath_tomorrow)
    except:
        df1 = download_calls(tomorrow_str)
        
    # merge both
    calls = pd.concat([df0, df1]).sort_values(['vehicle_id','timestamp'])
    calls = calls[calls.service_date == int(today_str.replace("-",""))]
    return calls

## get at stop buses
def get_at_stop(calls):
    calls = calls.sort_values(['trip_id', 'dist_along_route'])
    calls = calls[calls.dist_along_route != '\N']
    calls.drop_duplicates(subset = ['timestamp', 'trip_id'], inplace = True)
    calls.drop_duplicates(subset = ['dist_along_route', 'trip_id'], inplace = True)
    calls = calls[calls.progress == 0]
    calls['dist_along_route'] = calls.dist_along_route.astype('float')
    calls['dist_from_stop'] = calls.dist_from_stop.astype('float')
# distance in meters
    calls['dist_travel'] = calls.dist_along_route - calls.dist_along_route.shift(1)
    calls['dist_travel'] = calls.dist_travel * 0.00062137 # convert meters to miles
    calls = calls[calls['dist_travel'] > 0]
    calls['time_travel'] = calls.timestamp.astype('datetime64[ms]') - \
                            calls.timestamp.astype('datetime64[ms]').shift(1)
    calls['time_travel'] = calls.time_travel.apply(lambda x: x.total_seconds() / 60. / 60.)
    calls = calls[calls['time_travel'] > 0]
    calls['speed'] = calls['dist_travel'] / calls['time_travel']
    mask = calls.trip_id != calls.trip_id.shift(1)
    calls.loc[mask, 'dist_travel'] = -1
    calls.loc[mask, 'time_travel'] = -1
    calls.loc[mask, 'speed'] = -1
    calls = calls[calls.speed < 30]
    calls['to_stop'] = calls['dist_from_stop'] * 0.00062137 / calls.speed * 60.
    calls['deltas'] = calls.to_stop.apply(lambda x: timedelta(minutes = x))
    calls['timestamp2'] = calls['timestamp'].astype('datetime64[ms]') +  calls['deltas']
    # convert to EST
    calls['timestamp2'] = calls.timestamp2 - timedelta(hours = 4)
    clean_calls = calls[['trip_id', 'timestamp2', 'next_stop_id']]
    return clean_calls

def get_necessary_columns(clean_calls, trips):
    # merge calls and trips
    print "Merge calls and trips..."
    merged = pd.merge(clean_calls, trips, how='left', on='trip_id')
    merged = merged[['trip_id', 'next_stop_id', 'timestamp2', 'route_id', 'direction_id']]
    merged.dropna(inplace = True)
    merged['rds_index'] = merged.route_id.astype(str) + '_' + merged.direction_id.astype(int).astype(str) + '_' \
                            + merged.next_stop_id.astype(str)
    merged = merged.rename(columns = {'timestamp2': 'timestamp'})
    merged = merged[['trip_id', 'rds_index', 'timestamp']]
    outname = 'observed_headways_'+ today_str +'.csv'
    outputdir = join('calls', today_str[:7])
    if not os.path.exists(outputdir):
        os.mkdir(outputdir)
    merged.to_csv(join(outputdir, outname), index=False)
    return outname
    
def main():
    start = dt.datetime.now()
    print "DATE: %s" %today_str
    print "LOAD CALLS:"
    calls = get_calls(today_str)
    print "----------"
    print "GET ONLY BUSES AT STOP:"
    clean_calls = get_at_stop(calls)
    print "----------"
    print "REMOVE UNNECESSARY STOPS:"
    outname = get_necessary_columns(clean_calls, trips)
    print "----------"
    print "CALCULATE HEADWAYS:"
    print "----------"
    print "DONE! Output stored as %s" %outname
    end = dt.datetime.now()
    print (end-start)

main()