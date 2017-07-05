import os, sys
import pandas as pd
from datetime import datetime
from datetime import timedelta
import re
import json
import numpy as np

datadirectory = 'calls/'

# test year/day/month

def dowSchedName(month, day, year):
    '''
    function to find appropriate schedules for day of the week
    month, day and year are given as integers
    '''
    dow = datetime.datetime(year, month, day).weekday()
    if dow == 1:
        fn = 'monday.csv'
    elif (dow > 1) & (dow < 4):
        fn = 'tues_to_thurs.csv'
    elif dow == 5:
        fn = 'fri.csv'
    elif dow == 6:
        fn = 'sat.csv'
    elif dow == 7:
        fn = 'sun.csv'
    return fn

def getSched(dir_name, fn):
    sched = pd.read_csv(dir_name + '/' + fn, parse_dates = ['arrival_time'])
    return sched

def calcHeadway(df, timecol, headwaycol):
    '''
    Formula to calculate headway
    Takes a dataframe, the name of the time column, name of proposed headway column
    returns dataframe with new column

    '''
    # sort values by rds index & time
    # calculate the difference between each sequential row
    df = df.sort_values(['rds_index', timecol])
    df[headwaycol] = df[timecol].diff()
    # convert to seconds
    df[headwaycol] = df[headwaycol].apply(lambda x: x if type(x) == type(pd.NaT)
                    else x.total_seconds() / 60.0)
    # go back and revise hw for first bus at each rds to -1 (will be dropped)
    mask = df['rds_index'] != df['rds_index'].shift(1)
    df.loc[mask, headwaycol] = -1
    return df

def excessWaitTime(df, groupby_col = 'rds_index', time_param = None):
    '''
    Formula to calculate excess wait time

    Takes dataframe
    Groupby column ('rds_index' for stop level, 'route index' for route level)
    time_param:

    # 7-10 am : 1
    # 10-16: 2
    # 16-19: 3
    # 19-22: 4

    '''
    if time_param:
        df = df[df.time_period == time_param]

    observed_hw = df.groupby(groupby_col).sum()['obs_hw'] * 2.0
    scheduled_hw = df.groupby(groupby_col).sum()['sched_hw'] * 2.0
    sq_observed_hw = df.groupby(groupby_col).sum()['obs_hw_sq'] * 1.0
    sq_scheduled_hw = df.groupby(groupby_col).sum()['sched_hw_sq'] * 1.0
    swt = sq_scheduled_hw / scheduled_hw
    awt = sq_observed_hw / observed_hw
    ewt = awt - swt
    #print ewt
    return ewt

def getScheds(dow):
    '''
    Function to pull schedule for trip ids not in current schedule from prior
    months' schedules
    Returns dataframe containing all scheduled stops for prior trip ids
    '''
    boros = ['bronx', 'brooklyn', 'queens', 'manhattan', 'staten_island', 'busco']
    oldsched = pd.DataFrame()
    for boro in boros:
        for direc in os.walk('schedules/' + boro).next()[1]:
            filepath = 'schedules/' + boro + '/' + direc
            #print filepath
            temp = getSched(filepath, dow)
            # inner join to extract desired trip_ids
            temp = temp.merge(tripids, how = 'inner')
            oldsched = oldsched.append(temp)
    return oldsched


# read in call data
fn = datadirectory + 'observed_headways_{0}-{1}-{2}.csv'
fn = fn.format(str(year), str(month).zfill(2), str(day).zfill(2))
calls = pd.read_csv(fn, parse_dates = ['timestamp'])

# read in schedule
sched = getSched('schedules', dowSchedName(month, day, year))

# Obtain schedule for trips in gtfs only
# this also involves going into past schedules
tripids = pd.DataFrame({'trip_id': calls.trip_id.unique(), 'in_gtfs': 1})
sched2 = sched.merge(tripids, how = 'right')
oldsched = getScheds(dowSchedName(month, day, year))
combinedsched = sched2.append(oldsched)

# prints errors if not all trips are accounted for
if len(np.setdiff1d(combinedsched.trip_id.unique(), tripids.trip_id.unique())) > 0:
    print '{} Trips missing from schedule'.format(len(np.setdiff1d(combinedsched.trip_id.unique(),
            tripids.trip_id.unique()))
elif len(np.setdiff1d(tripids.trip_id.unique(), combinedsched.trip_id.unique())) > 0:
    print '{} Schedules missing'.format(len(np.setdiff1d(tripids.trip_id.unique(),
            combinedsched.trip_id.unique()))

# outer join calls data with schedule data
imptcols = ['arrival_time', 'rds_index', 'route_id', 'service_id', 'stop_id',
            'stop_sequence', 'trip_id']
merged = calls[['rds_index', 'trip_id', 'timestamp']].merge(combinedsched[imptcols],
            on = ['trip_id', 'rds_index'], how = 'outer')

# % of datapoints that are missing for some reason or another .5 % of data drops out
faildata = len(merged[merged.arrival_time.isnull()]) / float(len(merged)) * 100
print '{}% of data does not have an arrival time'.format(faildata)

def generateTime(hour):
    '''
    function to generate
    7-10 am : 1
    10-16 pm : 2
    16-19 pm : 3
    19- 22: 4
    ignoring late nights for these metrics : 5
    '''
    if (hour > 6) & (hour < 11):
        return 1
    elif (hour > 10) & (hour < 17):
        return 2
    elif (hour > 16) & (hour < 20):
        return 3
    elif (hour > 19) & (hour < 23):
        return 4
    else:
        return 5

merged['timeperiod'] = merged['arrival_time'].apply(lambda x: generateTime(x))

# for now, create 2ndary timestamp just for hw calculating sort purposes
# these fake hws will be overwritten with schedule hw(?)
# and/or fixed in the future through better joining
merged = merged.sort_values('arrival_time')
merged['timestamp2'] = merged.timestamp.ffill()

# for now drop duplicates where rds index & arrival time are the time
merged.drop_duplicates(subset = ['rds_index', 'arrival_time'], inplace = True)

merged = calcHeadway(merged, 'timestamp2', 'obs_hw')
merged = calcHeadway(merged, 'arrival_time', 'sched_hw')

obs_hw = [merged.sched_hw[i] if type(merged.obs_hw[i]) == type(pd.NaT) else
                merged.obs_hw[i] for i in merged.index]

merged.obs_hw = obs_hw

merged.sched_hw = merged.sched_hw.apply(lambda x: float(x))
merged.obs_hw = merged.obs_hw.apply(lambda x: float(x))

# remove latenights and those hws set = to -1 previously b/c first at stop
merged = merged[merged.timeperiod < 5]
merged = merged[(merged.obs_hw > 0) & (merged.sched_hw > 0)]

# lit suggests that this is best used on frequent routes
# define frequent routes as 5 buses per hour (1 every 12 minutes)
#also remove observations over 30 minutes as errors
# reconsider error band at future date
merged = merged[merged.sched_hw <= 12]
merged = merged[merged.obs_hw < 30]

# create squared columns for ewt calculation
merged['obs_hw_sq'] = merged.obs_hw.apply(lambda x: x**2)
merged['sched_hw_sq'] = merged.sched_hw.apply(lambda x: x**2)

# run all the functions

#stop level
ewt = excessWaitTime(merged)


#route level
ewt_route = excessWaitTime(merged, 'route_id')
