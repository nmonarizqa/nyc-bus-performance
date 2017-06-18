'''
Gather all trips.txt and stops.txt from transitfeeds
'''

import urllib2
import os
from os.path import join, isfile, abspath
import pandas as pd

schedule_path = join(os.getcwd(),"data/trips")
paths = {'trips': join(os.getcwd(),"data/stops"),
         'stops': join(os.getcwd(),"data/datelist.txt")}

datelist = [line.strip() for line in open(datelist_path, 'r')]
server = 'https://transitfeeds-data.s3-us-west-1.amazonaws.com/public/feeds/mta/'
endpoints = {'trips':'/original/trips.txt',
            'stops':'/original/stops.txt'}

# get trips
# doctype can only be "trips" or "stops
def get_data(doctype):  
    # download data
    err = []
    for i in range(80,86):
        for date in datelist:
            url = server + str(i) + "/" + date + endpoints[doctype]
            fname = date + "-" + str(i) + '.txt'
            try:
                response = urllib2.urlopen(url)
                with open(join(paths[doctype], fname), 'w') as f:
                    f.write(response.read())
                f.close()
            except:
                err.append(fname)

def combine_all_data(doctype):
    # combine all data       
    all_entities = pd.DataFrame()
    fnames = os.listdir(paths[doctype])
    for f in fnames:
        try:
            doc = pd.read_csv(join(schedule_path,f))
            if doctype == "stops":
                cols = ['stop_id','stop_lat','stop_lon','stop_name']
            else:
                cols = ['route_id','trip_id','direction_id']
            doc = doc[cols]
            all_entities = pd.concat([all_entities,docs])
        except:
            continue
    return all_entities

def main():
    get_data("trips")
    all_trips = combine_all_data("trips")
    all_trips = all_trips.loc[:,'route_id':'direction_id'].drop_duplicates()
    all_trips.to_csv(join(schedule_path,"all_trips.csv"), index=False)
    
    get_data("stops")
    all_stops = combine_all_data("stops")
    all_stops.drop_duplicates().to_csv(join(stop_path,"all_stops.csv"), index=False)
    
main()
