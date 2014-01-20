#!/usr/bin/env python -W ignore::DeprecationWarning

import calendar
import json
import os
import sys

from datetime import datetime

import pandas as pd
pd.options.display.width = 130
pd.options.display.max_rows = 100

import FailoverLib as fl

def main():

    geoip_database_file = "~/scripts/geolist/GeoIPOrg.dat"
    config_file = "instance_config.json"

    server_lists = "http://wlcg-squid-monitor.cern.ch/"
    geolist_file = server_lists + "geolist.txt"
    exception_list_file = server_lists + "exceptionlist.txt"

    geo = fl.parse_geolist( fl.get_url( geolist_file))
    actions, WN_view, MO_view = fl.parse_exceptionlist( fl.get_url( exception_list_file))
    squids = fl.build_squids_list(geo, MO_view)
    geoip = fl.GeoIPWrapper( os.path.expanduser( geoip_database_file))

    config = json.load( open(config_file))
    json.dump(config, open(config_file, 'w'), indent=3)

    groups = config['groups']
    for machine_group_name in groups.keys():
        analyze_failovers_to_group( config, machine_group_name, geo, squids, geoip)

    return 0

def analyze_failovers_to_group (config, groupname, geo, squids, geoip):

    groupconf = config['groups'][groupname]

    instances = groupconf['instances']
    last_stats_file = groupconf['file_last_stats']
    record_file  = groupconf['file_record']
    site_rate_threshold = groupconf['rate_threshold']   # Unit: Queries/sec
    record_span = config['history']['span']

    now = datetime.utcnow()

    awdata = fl.load_aggregated_awstats_data(instances)

    last_timestamp, last_awdata = load_last_data(last_stats_file)
    save_last_data(last_stats_file, awdata, now)

    if last_awdata is None:
        return 1

    if len(awdata) > 0:
        awdata = compute_traffic_delta( awdata, last_awdata, now, last_timestamp)
        awdata.insert( 0, 'IsSquid', awdata.index.isin(squids.Host) )
        awdata = add_institutions( awdata, geoip.get_isp)
        offending, totals_high = excess_failover_check( awdata, squids, site_rate_threshold)

    else:
        offending = None
        totals_high = None

    geolist_func = lambda s: s.encode('utf-8', 'ignore').replace(' ', '')
    get_sites = lambda inst: ', '.join( geo[ geo['Institution'] == geolist_func(inst) ]['Site'].unique().tolist() )

    gen_report (groupname, geo, offending, totals_high, get_sites)
    update_record (record_file, offending, now, record_span, get_sites)

    return 0

def load_last_data (last_stats_file):

    if os.path.exists(last_stats_file):
        fobj = open(last_stats_file)
        last_timestamp = datetime.utcfromtimestamp( int( fobj.next().strip()))
        fobj.close()

        last_awdata = pd.read_csv(last_stats_file, skiprows=1, index_col=0)

    else:
        last_awdata = None
        last_timestamp = None

    return last_timestamp, last_awdata

def save_last_data (last_stats_file, data, timestamp):

    fobj = open(last_stats_file, 'w')
    fobj.write( str(datetime_to_UTC_epoch(timestamp)) + '\n' )
    data.to_csv(fobj)
    fobj.close()

def datetime_to_UTC_epoch (dt):

    return calendar.timegm( dt.utctimetuple())

def compute_traffic_delta (now_stats, last_stats, now_timestamp, last_timestamp):

    delta_t = datetime_to_UTC_epoch(now_timestamp) - datetime_to_UTC_epoch(last_timestamp)
    delta_h = (now_stats['Hits'] - last_stats['Hits']) #.fillna(now_stats['Hits'])
    change = delta_h / float(delta_t)

    hits_column_idx = now_stats.columns.tolist().index('Hits')
    table = now_stats.copy()
    table.insert( hits_column_idx + 1, 'HitsRate', change)
    table = table.dropna()
    print hits_column_idx, table

    return table

def add_institutions (awstats_dataframe, isp_func):

    xaw = pd.DataFrame(awstats_dataframe.reset_index()['Host'])
    xaw['Institution'] = xaw.Host.apply(isp_func)
    xaw.set_index('Host', inplace=True)
    updated = pd.merge(awstats_dataframe, xaw, left_index=True, right_index=True)

    return updated

def excess_failover_check (awdata, squids, site_rate_threshold):

    non_squid_stats = awdata[ ~awdata['IsSquid'] ]

    by_institution = non_squid_stats.groupby('Institution')
    totals = by_institution[['HitsRate', 'Bandwidth']].sum()
    totals_high = totals[ totals['HitsRate'] > site_rate_threshold ]

    offending = awdata[ awdata['Institution'].isin(totals_high.index) ].reset_index()
    squid_alias_map = squids.set_index('Host')['Alias']
    offending['Host'][offending['IsSquid']] = offending['Host'][offending['IsSquid']].map(squid_alias_map)

    return offending, totals_high

def gen_report (groupname, geo, offending, totals_high, sites_function):

    print "Failover activity to %s:" % groupname

    if offending is None:
        print " None.\n"
        return

    for_report = offending.copy()

    if len(for_report) > 0:
        for_report['Sites'] = for_report['Institution'].apply(sites_function)
        print for_report.set_index(['Institution', 'IsSquid', 'Host']).sortlevel(0), "\n"

    else:
        print " None.\n"

def update_record (record_file, new_data, now, record_span, sites_function):

    time_field = 'Timestamp'

    now_secs = datetime_to_UTC_epoch(now)

    if new_data is None:
        return

    if os.path.exists(record_file):
        records = pd.read_csv(record_file)
        old_cutoff = now_secs - record_span*3600
        records = records[ records[time_field] >= old_cutoff ]

    else:
        records = None

    to_add = new_data.copy()
    to_add['Sites'] = to_add['Institution'].apply(sites_function)
    to_add = to_add.drop('Institution', axis=1)
    to_add.insert(0, time_field, now_secs)

    if records:
        update = pd.concat([records, to_add])
    else:
        update = to_add

    update.to_csv(record_file, index=False)


if __name__ == "__main__":
    sys.exit(main())
