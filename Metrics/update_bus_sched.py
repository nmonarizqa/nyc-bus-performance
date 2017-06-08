# keep schedules up to date
import os, sys
import pandas as pd
import requests
import zipfile as zf
import StringIO
from datetime import datetime
from datetime import timedelta
import re
import json
datadirectory = 'C:\Users\Nurvirta\OneDrive\CUSP\Capstone'

a = datetime.now()
def getSched():
    bx = 'http://web.mta.info/developers/data/nyct/bus/google_transit_bronx.zip'
    bk = 'http://web.mta.info/developers/data/nyct/bus/google_transit_brooklyn.zip'
    mn = 'http://web.mta.info/developers/data/nyct/bus/google_transit_manhattan.zip'
    qn = 'http://web.mta.info/developers/data/nyct/bus/google_transit_queens.zip'
    si = 'http://web.mta.info/developers/data/nyct/bus/google_transit_staten_island.zip'
    buscomp = 'http://web.mta.info/developers/data/busco/google_transit.zip'
    boros = [bx, bk, mn, qn, si, buscomp]
    sched = pd.DataFrame()
    enddate = {}
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
        temp = pd.read_csv(datadirectory + '/schedules/' + name + '/stop_times.txt')
        trips = pd.read_csv(datadirectory + '/schedules/' + name + '/trips.txt')
        cal = pd.read_csv(datadirectory + '/schedules/' + name + '/calendar.txt')
        enddate[name] = max(cal.end_date)
        print "Merging data..."
        temp = temp.merge(trips[['route_id', 'trip_id', 'direction_id', 'service_id']], how = 'left')
        temp = temp.merge(cal)
        sched = sched.append(temp)
    sched['rds_index'] = sched['route_id'] + sched['direction_id'].astype(str) + sched['stop_id'].astype(str)
    print "Finish downloading data."
    return sched, enddate

scheds, enddate = getSched()

# scheduled calls
scheds.reset_index(inplace = True, drop = True)

daysofweek = ['arrival_time', 'monday', 'tuesday', 'wednesday', 'thursday',
                'friday', 'saturday', 'sunday']
daysofweek_loop = daysofweek[1:7]
daysofweek_loop.insert(0, 'sunday')
daysofweek_loop.insert(0, 'arrival_time')

# does anyone know a faster way to make this happen?
print "Get days of week..."
scheds[daysofweek] = scheds[daysofweek].apply(lambda x: list(x[daysofweek_loop]) if
                        x.arrival_time > '23:59:59' else x, axis = 1)
print "Convert arrival time..."
scheds.arrival_time = scheds.arrival_time.apply(lambda x: datetime.strptime(x, '%H:%M:%S') if int(x[:2]) < 24
                    else datetime.strptime(x.replace(x[:2], str(int(x[:2])-24).zfill(2), 1), '%H:%M:%S'))

mon = scheds[scheds.monday == 1].sort_values(['rds_index', 'arrival_time'])
tues_to_thurs = scheds[scheds.tuesday == 1].sort_values(['rds_index', 'arrival_time'])
fri = scheds[scheds.friday == 1].sort_values(['rds_index', 'arrival_time'])
sat = scheds[scheds.saturday == 1].sort_values(['rds_index', 'arrival_time'])
sun = scheds[scheds.sunday == 1].sort_values(['rds_index', 'arrival_time'])


# calculate time difference between each row
# if first item in rds, get last arrivaltime from previous day
# NOTE TO SELF: DEAL WITH SCHEDULE CHANGE DAY
# NOTE TO SELF: DEAL WITH HANDFUL OF STOPS THAT HAVE NO HEADWAY
print "Calculate headways..."
dfs = [mon, tues_to_thurs, fri, sat, sun]
for i, df in enumerate(dfs):
    df['headways'] = df.arrival_time.diff()
    df['headways'] = df['headways'].apply(lambda x: x.seconds / 60.0)
    if i < 4:
        df2 = dfs[i + 1]
    else:
        df2 = dfs[0]
    mask = df['rds_index'] != df['rds_index'].shift(1)
    masked = df[mask].merge(df2.groupby('rds_index', as_index = False)\
            .max()[['rds_index', 'arrival_time']], on='rds_index', how = 'left')
    diff = [masked['arrival_time_x'][i] - masked['arrival_time_y'][i] + timedelta(hours = 24)
            for i in masked.index]
    diff = [x.seconds / 60.0 for x in diff]
    df.loc[mask, 'headways'] = diff


print "Exporting data..."
mon.to_csv('schedules/monday.csv')
tues_to_thurs.to_csv('schedules/tues_to_thurs.csv')
fri.to_csv('schedules/fri.csv')
sat.to_csv('schedules/sat.csv')
sun.to_csv('schedules/sun.csv')

json.dump(enddate, open('schedules/enddate.txt', 'w+'))
b = datetime.now()
print "DONE!"
print b-a
