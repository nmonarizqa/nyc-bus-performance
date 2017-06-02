# keep schedules up to date
import os, sys
import pandas as pd
import geopandas as gp
import requests
import zipfile as zf
import StringIO
from google.transit import gtfs_realtime_pb2 as gtfs
import lzmaffi as lzma
from datetime import datetime
import re
datadirectory = '/Users/shay/CUSP/transitcenter_capstone/data/'

# is there a way to add test to only download is newer version is available?
def getSched():
    bx = 'http://web.mta.info/developers/data/nyct/bus/google_transit_bronx.zip'
    bk = 'http://web.mta.info/developers/data/nyct/bus/google_transit_brooklyn.zip'
    mn = 'http://web.mta.info/developers/data/nyct/bus/google_transit_manhattan.zip'
    qn = 'http://web.mta.info/developers/data/nyct/bus/google_transit_queens.zip'
    si = 'http://web.mta.info/developers/data/nyct/bus/google_transit_staten_island.zip'
    buscomp = 'http://web.mta.info/developers/data/busco/google_transit.zip'
    boros = [bx, bk, mn, qn, si, buscomp]
    sched = pd.DataFrame()
    for boro in boros:
        if boro != buscomp:
            name = re.search(r'transit_(.*?).zip', boro).group(1)
        else:
            name = buscomp
        # r = requests.get(boro, stream = True)
        # z = zf.ZipFile(StringIO.StringIO(r.content))
        # z.extractall(datadirectory + '/schedules/' + name)
        temp = pd.read_csv(datadirectory + '/schedules/' + name + '/stop_times.txt')
        trips = pd.read_csv(datadirectory + '/schedules/' + name + '/trips.txt')
        cal = pd.read_csv(datadirectory + '/schedules/' + name + '/calendar.txt')
        temp = temp.merge(trips[['route_id', 'trip_id', 'direction_id', 'service_id']], how = 'left')
        temp = temp.merge(cal, how = 'left')
        sched = sched.append(temp)
    sched['rds_index'] = sched['route_id'] + sched['direction_id'].astype(str) + sched['stop_id'].astype(str)
    return sched

scheds = getSched()

# scheduled calls
scheds.head()

# have to add a day for these...
# calculate headway
scheds.arrival_time = scheds.arrival_time.apply(lambda x: datetime.strptime(x, '%H:%M:%S') if int(x[:2]) < 24
                    else datetime.strptime(x.replace(x[:2], str(int(x[:2])-24).zfill(2), 1), '%H:%M:%S'))
