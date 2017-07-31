import os, sys
import pandas as pd
from datetime import datetime
from datetime import timedelta
import re
import numpy as np
from string import digits

year = int(sys.argv[1])
month = int(sys.argv[2])
day = int(sys.argv[3])

print '{}/{}/{}'.format(month, day, year)

datadirectory = 'calls/{}-{}/'.format(year, str(month).zfill(2))


## functions to combine schedule with gtfs


def mergeCallsWScheds(month, day, year):

    def dowSchedName(month, day, year):
        '''
        function to find appropriate schedules for day of the week
        month, day and year are given as integers
        '''
        dow = datetime(year, month, day).weekday()
        if dow == 0:
            fn = 'monday.csv'
        elif (dow > 0) & (dow < 4):
            fn = 'tues_to_thurs.csv'
        elif dow == 4:
            fn = 'fri.csv'
        elif dow == 5:
            fn = 'sat.csv'
        elif dow == 6:
            fn = 'sun.csv'
        return fn

    def getSched(dir_name, fn):
        sched = pd.read_csv(dir_name + '/' + fn, parse_dates = ['arrival_time'])
        return sched

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
                if direc[:4] <= str(year):
                    filepath = 'schedules/' + boro + '/' + direc
                    try:
                        temp = getSched(filepath, dow)
                    # inner join to extract desired trip_ids
                        temp = temp.merge(tripids, how = 'inner')
                        oldsched = oldsched.append(temp)
                    except:
                        continue
        return oldsched

    # read in call data
    fn = datadirectory + 'observed_headways_{0}-{1}-{2}.csv'
    fn = fn.format(str(year), str(month).zfill(2), str(day).zfill(2))
    calls = pd.read_csv(fn, parse_dates = ['timestamp'])

    # read in schedule
    sched = getSched('schedules', fn = dowSchedName(month, day, year))

    # Obtain schedule for trips in gtfs only
    # this also involves going into past schedules
    tripids = pd.DataFrame({'trip_id': calls.trip_id.unique(), 'in_gtfs': 1})
    sched2 = sched.merge(tripids, how = 'right')
    oldsched = getScheds(dowSchedName(month, day, year))
    combinedsched = sched2.append(oldsched)
    # outer join calls data with schedule data
    imptcols = ['arrival_time', 'rds_index', 'route_id', 'stop_sequence', 'trip_id']
    merged = calls[['rds_index', 'trip_id', 'timestamp']].merge(combinedsched[imptcols],
                on = ['trip_id', 'rds_index'], how = 'outer')

    # % of datapoints that are missing for some reason or another .5 % of data drops out
    faildata = len(merged[merged.arrival_time.isnull()]) / float(len(merged)) * 100
    faildata2 = len(merged[merged.timestamp.isnull()]) / float(len(merged)) * 100
    print '{}% of data does not have an arrival time'.format(faildata)
    print '{}% of data does not have a timestamp'.format(faildata2)
    merged = merged[~merged.arrival_time.isnull()]
    return merged


## manipulations for metrics

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

def removeTrips(df):
    # do not calculate on latenights
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
        elif (hour > 19) & (hour < 22):
            return 4
        else:
            return 5

    df['timeperiod'] = df['arrival_time'].apply(lambda x: generateTime(x.hour))
    df = df[df.timeperiod < 5]
    # literature suggests that ewt is only valid for trips with
    # remove trips with no recorded timestamps in data
    #dropmask = df.groupby('trip_id').timestamp.min().isnull()
    #dropmask = dropmask[dropmask == True]
    #removetrips = list(dropmask.keys().values)
    #df['remove'] = df.trip_id.apply(lambda x: 0 if x in removetrips else 1)
    #df = df[df.remove > 0]
    #df.drop('remove', axis = 1, inplace = True)
    df['difference'] = df['arrival_time'] - df['timestamp2']
    df['difference'] = df.difference.apply(lambda x: x.seconds / 60.)
    df['difference'] = df.difference.apply(lambda x: min(x, 1440 - x))
    # remove points where the difference between schedule and observed is greater than 40 minutes
    # we are mostly dealing with high frequency bus routes so this typically indicates an error in data
    df = df[df.difference <= 40]
    return df

def excessWaitTime(dataframe, groupby_col, time_param):
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
    df = dataframe.copy()
    # remove points set to -1 because new trip
    df = df[(df.obs_hw > 0) & (df.sched_hw > 0)]
    df = df[(df.sched_hw <= 15) & (df.obs_hw <= 30)]
    # filter by time period or set time period = 0 if all times
    if time_param == 'all':
        df['timeperiod'] = 0
    
    grouped = df.groupby([groupby_col, 'timeperiod'])

    if groupby_col == 'rds_index':
        awt = grouped.mean()['obs_hw_sq'] / (2. * grouped.mean()['obs_hw'])
        swt = grouped.mean()['sched_hw_sq'] / (2. * grouped.mean()['sched_hw'])
        awt_95 = grouped.quantile(.95)['obs_hw'] / 2.
        swt_95 = grouped.median()['sched_hw'] / 2.
        ewt_95 = awt_95 - swt_95
        counts = grouped.count()['sched_hw']
        results = pd.concat([awt, swt, ewt_95, counts], axis = 1)
        results.reset_index(inplace = True)
        results.columns = ['rds_index', 'hourbin', 'awt', 'swt', 'ewt_95', 'count']
        results['awt'] = results['awt'].apply(lambda x: round(x, 2))
        results['swt'] = results['swt'].apply(lambda x: round(x, 2))
        results['ewt_95'] = results['ewt_95'].apply(lambda x: round(x, 2))
    else:
        awt = grouped.mean()['obs_hw_sq'] / (2. * grouped.mean()['obs_hw'])
        swt = grouped.mean()['sched_hw_sq'] / (2. * grouped.mean()['sched_hw'])
        ewt = awt - swt
        counts = grouped.count()['sched_hw']
        results = pd.concat([ewt, counts], axis = 1)
        results.reset_index(inplace = True)
        results.columns = ['rds_index', 'hourbin', 'ewt', 'count']
        results['ewt'] = results['ewt'].apply(lambda x: round(x, 2))
    return results


# interpolate stops with multiple missing
def interpolateStopTime(df):
    df = df.sort_values(['trip_id', 'arrival_time'])
    df.reset_index(inplace = True, drop = True)
    mask = df.trip_id.shift(1) != df.trip_id
    splits = np.split(df.timestamp, mask[mask == True].index)
    def nan_helper(y):
        return y<0, lambda z: z.nonzero()[0]
    def interpolateStops(group):
        arrival_time_int = group.astype(np.int64)
        nans, x = nan_helper(arrival_time_int)
        arrival_time_int[nans]= np.interp(x(nans), x(~nans), arrival_time_int[~nans])
        return arrival_time_int.astype('datetime64[ns]')
    newtimes = [interpolateStops(split) if (len(split[split.isnull()]) > 0) & (len(split[~split.isnull()]) > 1)
                else split for split in splits[1:]]
    df['timestamp2'] = pd.concat(newtimes)
    df.dropna(subset = ['timestamp2'], inplace = True)
    df.drop_duplicates(subset = ['timestamp2', 'trip_id'], inplace = True)
    return df

def getBetweenStopTime(df):
    df = df.sort_values(['trip_id', 'stop_sequence'])
    mask = df['trip_id'].shift(1) != df['trip_id']
    diff = np.ediff1d(df.timestamp2).astype('timedelta64[s]').astype('float') / 60.
    diff = np.insert(diff, 0, 0)
    df['journeytime'] = diff
    df.loc[mask, 'journeytime'] = 0
    diff_arr = np.ediff1d(df.arrival_time).astype('timedelta64[s]').astype('float') / 60.
    diff_arr = np.insert(diff_arr, 0, 0)
    df['scheduledtrip'] = diff_arr
    df.loc[mask, 'scheduledtrip'] = 0
    # sometimes there are errors where a gtfs timestamp for a bus at stop sequence x is earlier than that at stop
    # sequence x - 1. Therefore, remove those from the calculation
    df = df[df.journeytime >= 0]
    df = df[df.scheduledtrip >= 0]
    #convert to minutes
    return df


def reliabilityBufferTime(dataframe, groupbycol, timeparam):
    df = dataframe.copy()
    if groupbycol == 'r_index' or groupbycol == 'rd_index': 
        if timeparam == 'all':
            df['timeperiod'] = 0
        cols = ['r_index', 'rd_index', 'rds_index', 'timeperiod', 'journeytime', 'scheduledtrip']
        df = df[cols]
        grouped = df.groupby(['rds_index', 'rd_index', 'r_index'], as_index = False)
        journeymed = grouped.median().groupby(['rd_index', 'r_index'], as_index = False).sum()
        journey95 = grouped.quantile(.95).groupby(['rd_index', 'r_index']).sum()
        journey95.reset_index(inplace = True)
        # get sum of median journey time across route
        if groupbycol == 'r_index':
            journeymed = journeymed.groupby('r_index', as_index = False).mean()
            journey95 = journey95.groupby('r_index', as_index = False).mean()
        rbt = journey95['journeytime'] - journeymed['journeytime']
        results = pd.concat([journeymed[[groupbycol, 'timeperiod']], rbt], axis = 1)
        results.columns = ['rds_index', 'hourbin', 'rbt']
        results['rbt'] = results['rbt'].apply(lambda x: round(x, 2))
    else:
        if timeparam == 'all':
            df.timeperiod = 0
        grouped = df.groupby([groupbycol, 'timeperiod'])
        trip_95 = grouped.quantile(.95)['journeytime']
        m_trip = grouped.mean()['journeytime']
        s_trip = grouped.mean()['scheduledtrip']
        results = pd.concat([s_trip, m_trip, trip_95], axis = 1)
        results.reset_index(inplace = True)
        results.columns = ['rds_index', 'hourbin', 's_trip', 'm_trip', 'trip_95']
        results['s_trip'] = results['s_trip'].apply(lambda x: round(x, 2))
        results['m_trip'] = results['m_trip'].apply(lambda x: round(x, 2))
        results['trip_95'] = results['trip_95'].apply(lambda x: round(x, 2))
    return results

def travelTime(dataframe, timeparam):
    df = dataframe.copy()
    if timeparam == 'all':
        df.timeperiod = 0
    grouped = df.groupby(['rds_index', 'timeperiod'])
    trip_95 = grouped.quantile(.95)['journeytime']
    m_trip = grouped.mean()['journeytime']
    s_trip = grouped.mean()['scheduledtrip']
    results = pd.concat([s_trip, m_trip, trip_95], axis = 1)
    results.reset_index(inplace = True)
    results.columns = ['rds_index', 'hourbin', 's_trip', 'm_trip', 'trip_95']
    results['s_trip'] = results['s_trip'].apply(lambda x: round(x, 2))
    results['m_trip'] = results['m_trip'].apply(lambda x: round(x, 2))
    results['trip_95'] = results['trip_95'].apply(lambda x: round(x, 2))
    return results

def speed(dataframe, groupby_col, timeparam):

    df = dataframe.copy()
    if timeparam == 'all':
        df['timeperiod'] = 0
    grouped = df.groupby([groupby_col, 'timeperiod'])
    speed = grouped.mean()['speed']
    results = pd.concat([grouped.mean()['journeytime'], speed], axis = 1)
    results.reset_index(inplace = True)
    results.drop('journeytime', axis = 1, inplace = True)
    results = results.rename(columns = {'timeperiod': 'hourbin'})                        
    results['speed'] = results.speed.apply(lambda x: round(x, 2))
    results = results[results.speed < 40]
    return results

def getAllRBT(df, groupbys):
    results = pd.DataFrame()
    for time in ['all', 'bin']:
        for groupby in groupbys:
            rez = reliabilityBufferTime(df, groupby, time)
            results = results.append(rez)
    return results

def getAllTravelTime(df):
    results = pd.DataFrame()
    for time in ['all', 'bin']:
        rez = travelTime(df, time)
        results = results.append(rez)
    return results

def getAllEWT(df, groupbys):
    results = pd.DataFrame()
    for time in ['all', 'bin']:
        for groupby in groupbys:
            if (time == 'bin') and (groupby == 'boro_index'):
                continue
            else:
                rez = excessWaitTime(df, groupby, time)
                results = results.append(rez)
    return results

def getAllSpeed(df, groupbys):
    results = pd.DataFrame()
    for time in ['all', 'bin']:
        for groupby in groupbys:
            if time == 'bin' and groupby == 'boro_index':
                continue
            else:
                rez = speed(df, groupby, time)
                results = results.append(rez)
    return results

def dayBin(year, month, day):
    dow = datetime(year, month, day).weekday()
    if dow < 5:
        return 1
    else:
        return 2

def grouping_cols(df):
    df['direction'] = [re.search(r'_(.*?)_', x).group(0) for x in df.rds_index]
    df['rd_index'] = df.route_id.astype(str) + df.direction + '0'
    df['r_index'] = df.route_id.astype(str) + '_2_0'
    df['boro_index'] = [x[:3].translate(None, digits).replace('+', '') + '_2_0' for x in df.route_id]
    return df

# run all the functions
def main():
    print datetime.today().time()
    
    merged = mergeCallsWScheds(month, day, year)
    merged.drop_duplicates(subset = ['rds_index', 'arrival_time'], inplace = True)
    print datetime.today().time()
    interpolated = interpolateStopTime(merged)
    interpolated = grouping_cols(interpolated)
    interpolated = removeTrips(interpolated)
    hws = calcHeadway(interpolated, 'timestamp2', 'obs_hw')
    hws = calcHeadway(hws, 'arrival_time', 'sched_hw')
    btstop = getBetweenStopTime(interpolated)
    hws['obs_hw'] = hws['obs_hw'].astype(float)
    hws['sched_hw'] = hws['sched_hw'].astype(float)
    hws['obs_hw_sq'] = hws.obs_hw ** 2
    hws['sched_hw_sq'] = hws.sched_hw ** 2
    hws = grouping_cols(hws)
    btstop = grouping_cols(btstop)
    routes = ['rd_index', 'r_index', 'boro_index']
    ewt_journey = getAllEWT(hws, ['rds_index'])
    rbt_journey = getAllTravelTime(btstop)
    ewt_route = getAllEWT(hws, routes)
    #rbt_route = getAllRBT(btstop, routes[:-1])
    print datetime.today().time()
    
    distances = pd.read_csv('data/2015-10-01_distance.csv'.format(str(year), str(month).zfill(2), str(day).zfill(2)))
    btstop = pd.merge(btstop, distances[['rds_index', 'distance']], how = 'left')
    btstop = btstop[btstop.journeytime > .5]
    btstop = btstop[~btstop.distance.isnull()]
    btstop['speed'] = btstop.distance / (btstop.journeytime / 60.)
    speed_route = getAllSpeed(btstop, routes)
    speed_journey = getAllSpeed(btstop, ['rds_index'])

    output_path = 'outputs/{0}-{1}/'.format(str(year), str(month).zfill(2))
    if not os.path.exists(output_path):
        os.makedirs(output_path + '/ewt')
        os.makedirs(output_path + '/rbt')
        os.makedirs(output_path + '/speed')

    for df in [ewt_route, ewt_journey, rbt_journey, speed_route, speed_journey]:
        df['daybin'] = dayBin(year, month, day)
        df['date'] = datetime(year, month, day).strftime('%m-%d-%Y')

    ewt_route.to_csv('outputs/{0}-{1}/ewt/{0}_{1}_{2}_ewtroute.csv'.format(str(year),
                    str(month).zfill(2), str(day).zfill(2)), index = False, na_rep = 'null')
    ewt_journey.to_csv('outputs/{0}-{1}/ewt/{0}_{1}_{2}_ewtjourney.csv'.format(str(year),
                    str(month).zfill(2), str(day).zfill(2)), index = False, na_rep = 'null')
    #rbt_route.to_csv('outputs/{0}-{1}/rbt/{0}_{1}_{2}_rbtroute.csv'.format(str(year),
    #                str(month).zfill(2), str(day).zfill(2)), index = False, na_rep = 'null')
    rbt_journey.to_csv('outputs/{0}-{1}/rbt/{0}_{1}_{2}_rbtjourney.csv'.format(str(year),
                    str(month).zfill(2), str(day).zfill(2)), index = False, na_rep = 'null')
    speed_route.to_csv('outputs/{0}-{1}/speed/{0}_{1}_{2}_speedroute.csv'.format(str(year), 
                    str(month).zfill(2), str(day).zfill(2)), index = False, na_rep = 'null')
    speed_journey.to_csv('outputs/{0}-{1}/speed/{0}_{1}_{2}_speedjourney.csv'.format(str(year),
                    str(month).zfill(2), str(day).zfill(2)), index = False, na_rep = 'null')
    print '{}/{}/{} done'.format(str(month).zfill(2), str(day).zfill(2), str(year))
    print datetime.today().time()

main()
