import pandas as pd
from datetime import datetime, timedelta
import requests
import zipfile as zf
import StringIO
import re
import os
import numpy as np

# get trips list
bx = 'http://web.mta.info/developers/data/nyct/bus/google_transit_bronx.zip'
bk = 'http://web.mta.info/developers/data/nyct/bus/google_transit_brooklyn.zip'
mn = 'http://web.mta.info/developers/data/nyct/bus/google_transit_manhattan.zip'
qn = 'http://web.mta.info/developers/data/nyct/bus/google_transit_queens.zip'
si = 'http://web.mta.info/developers/data/nyct/bus/google_transit_staten_island.zip'
buscomp = 'http://web.mta.info/developers/data/busco/google_transit.zip'

boros = [bx, bk, mn, qn, si, buscomp]
sched = pd.DataFrame()
enddate = {}
datadirectory = 'C:\Users\Nurvirta\OneDrive\CUSP\Capstone'
for boro in boros:
    if boro != buscomp:
        name = re.search(r'transit_(.*?).zip', boro).group(1)
    else:
        name = 'busco'
    print "Downloading data for boro %s..." %boro
    r = requests.get(boro, stream = True)
    z = zf.ZipFile(StringIO.StringIO(r.content))
    print "Extracting data for boro %s..." %boro
    z.extractall(datadirectory + '/schedules/' + name)

trips = pd.DataFrame()
for boro in os.listdir(datadirectory+"/schedules/"):
    path = datadirectory+"/schedules/"+boro
    trips_boro = pd.read_csv(path+"/trips.txt")
    trips = pd.concat([trips,trips_boro]).dropna()
print "Get trips... done!"


# get calls
# in real time, it should be:
# date = datetime.now().strftime("%Y-%m-%d")
# but the calls hasn't updated for a few days now
date = "2017-06-01"
server = "http://data.mytransit.nyc/bus_time"
year = date.split("-")[0]
yearmonth = date[:7]

# download and unzip calls
print "Download calls..."
path = "%s/%s/%s/bus_time_%s.csv.xz" %(server,year, yearmonth, date.replace("-",""))
destination = "calls/%s.csv.xz" %(date)
comm = "curl -o %s %s; unxz %s" %(destination, path, destination)
os.system(comm)

# read calls
print "Read calls..."
fname = destination[:-3]
calls = pd.read_csv(fname)

calls = calls[['timestamp','trip_id','next_stop_id','dist_from_stop']]
calls['timestamp'] = calls['timestamp'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ") - timedelta(hours=5))
calls=calls[calls['timestamp'] >= datetime(2017,6,1)]
def get_route(x):
    try:
        return x.split("_")[2]
    except:
        return np.nan
calls['route_id'] = calls.trip_id.apply(lambda x: get_route(x))
calls = calls.sort_values(["trip_id","timestamp"]).dropna()
calls.index = range(len(calls))

# select buses AT stop
print "Find buses at stop..."
current_stop = None
current_route = None
idx = []
for i in range(len(calls)):
    ent = calls.ix[i]
    if (ent.next_stop_id != current_stop) or (ent.route_id != current_route):
        idx.append(i-1)
    current_stop = ent.next_stop_id
    current_route = ent.route_id
    print '\r',"% buses at stop found",str((i+1)*100./len(calls))[:4],
calls = calls.ix[idx[1:]]

# calculate headways
print "Calculate headways..."
calls=calls.sort_values(['next_stop_id','route_id','timestamp'])
calls.index = range(len(calls))
current_stop = None
current_route = None
current_time = None
headways = []
for i in range(len(calls)):
    ent = calls.ix[i]
    if (ent.next_stop_id != current_stop) or (ent.route_id != current_route):
        headways.append(np.nan)
    else:
        hw = (ent.timestamp - current_time).seconds*1./60
        headways.append(hw)
    current_stop = ent.next_stop_id
    current_route = ent.route_id
    current_time = ent.timestamp
    print '\r',"% headways calculated",str((i+1)*100./len(calls))[:4],
calls['headways'] = headways
calls = calls.drop('dist_from_stop', axis=1)
calls = calls[calls.next_stop_id !='\N']
calls.to_csv("observed_headways.csv")
print "DONE!"
