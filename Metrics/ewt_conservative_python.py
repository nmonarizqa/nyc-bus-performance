import os, sys
import pandas as pd
import geopandas as gp
import requests
import zipfile as zf
import StringIO
from google.transit import gtfs_realtime_pb2 as gtfs
import lzmaffi as lzma

datadirectory = '/Users/shay/CUSP/transitcenter_capstone/data/'
busurl= 'http://web.mta.info/developers/data/nyct/bus/Bus_Shapefiles.zip'
routeurl = 'http://faculty.baruch.cuny.edu/geoportal/data/nyc_transit/jan2017/routes_bus_nyc_jan2017.zip'
# Note to self: shapefile data doesn't include express buses?

def getStopShape():
    '''downloads shapefile data for buses
    '''
    try:
        bustops = gp.read_file(datadirectory 'BusStopsAsOfMarch2.shp')
    except:
        r = requests.get(busurl, stream = True)
        z = zf.ZipFile(StringIO.StringIO(r.content))
        z.extractall(datadirectory)
        for
        os.rename(datadirectory )
        bustops = gp.read_file(datadirectory + 'BusStopsAsOfMarch2.shp')
    return bustops

def getRouteShape():
    '''downloads shapefile data for bus routes
    '''
    try:
        routeNYCT = gp.read_file(datadirectory + '/routes/NYCT Bus Routes.shp')
        routesMTA = gp.read_file(datadirectory + '/routes/MTA_Bus.shp')
        routes = routeNYCT.append(routesMTA)
    except:
        r = requests.get(busurl, stream = True)
        z = zf.ZipFile(StringIO.StringIO(r.content))
        z.extractall(datadirectory + '/routes')
        routeNYCT = gp.read_file(datadirectory + '/routes/NYCT Bus Routes.shp')
        routesMTA = gp.read_file(datadirectory + '/routes/MTA_Bus.shp')
        outes = routeNYCT.append(routesMTA)
    return routes
gp.read_file(datadirectory + '/routes/MTA_Bus_Stops.shp')


def getBusData(yr, mnth, day, datadirectory = datadirectory):
    '''
    takes yr in YYYY format
    '''

    yr = str(yr)
    mnth = str(mnth).zfill(2)
    day = str(day).zfill(2)
    date = yr + mnth + day
    if len(yr) != 4:
        print 'year must be 4 digits'
    if len(mnth) != 2:
        print 'mnth must be 2 digits'
    if len(day) != 2:
        print 'day must be 2 digits'

    fn = 'bus_time_' + date + '.csv'

    if os.path.exists(datadirectory + fn + '.xz'):
        print 'Already downloaded'
    else:
        baseurl = 'http://data.mytransit.nyc/bus_time/'
        url = baseurl + yr + '/' + yr + '-' + mnth + '/' + fn + '.xz'
        r = requests.get(url)
        with open(fn + '.xz', 'wb') as f:
            f.write(r.content)
            f.close()
        shutil.move(fn + '.xz', datadirectory)

def readMonthData(date, datadirectory = datadirectory):
    '''
    create csv using date in YYYYMMDD format '''

    date = str(date)

    fn = 'bus_time_' + date + '.csv'
    colnames = ['timestamp', 'vehicle_id', 'latitude', 'longitude', 'bearing',
                'progress', 'service_date', 'trip_id', 'block_assigned',
                 'next_stop_id', 'dist_along_route', 'dist_from_stop']
    try:
        df = pd.read_csv(datadirectory + fn)
    except:
        with lzma.open(datadirectory + fn + '.xz') as lz:
            with open(datadirectory + fn, 'wb') as fi:
                for line in lz:
                    fi.write(line)
            fi.close()
        lz.close()
        df = pd.read_csv(datadirectory + fn)
    return df

getBusData(2017, 01, 01)
jan2017 = readMonthData('20170101')

# attempt to mimic sql files from TransitCenter repo
# calculate observed headway
# sort by route/direction/stop and departure time
# identify stops ids with routes???

routes = getRouteShape()
stops = getStopShape()
