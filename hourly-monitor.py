#!/usr/bin/env python

import calendar
import json
import os
import sys
from datetime import datetime

import pandas as pd

import FailoverLib as fl

"""
TODO
     Implement exception for some French machines
"""
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
    now = datetime.utcnow()

    config = json.load( open(config_file))
    json.dump(config, open(config_file, 'w'), indent=3)

    groups = config['groups']
    record_file = config['record_file']
    record_span = config['history']['span']
    current_records = load_records(record_file, now, record_span)
    failover_groups = []

    for machine_group_name in groups.keys():

        past_records = get_group_records(current_records, machine_group_name)
        failover = analyze_failovers_to_group( config, machine_group_name, now,
                                               past_records, geo, squids, geoip )
        failover['Group'] = machine_group_name
        failover_groups.append(failover.copy())

    failover_record = pd.concat(failover_groups)
    failover_record.to_csv(record_file, index=False)

    return 0

def load_records (record_file, now, record_span):

    now_secs = datetime_to_UTC_epoch(now)
    old_cutoff = now_secs - record_span*3600

    if os.path.exists(record_file):
        records = pd.read_csv(record_file)
        records = records[ records['Timestamp'] >= old_cutoff ]
    else:
        records = None

    return records

def get_group_records (records, group_name):

    if records and 'Group' in records:
        return records[ records['Group'] == group_name ]
    else:
        return None

def analyze_failovers_to_group (config, groupname, now, past_records, geo, squids, geoip):

    groupconf = config['groups'][groupname]

    instances = groupconf['instances']
    last_stats_file = groupconf['file_last_stats']
    site_rate_threshold = groupconf['rate_threshold']   # Unit: Queries/sec

    awdata = fl.load_aggregated_awstats_data(instances)
    last_timestamp, last_awdata = load_last_data(last_stats_file)
    save_last_data(last_stats_file, awdata, now)

    if last_awdata is None:
        return 1

    awdata = compute_traffic_delta( awdata, last_awdata, now, last_timestamp)

    if len(awdata) > 0:
        awdata.insert( 0, 'IsSquid', awdata.index.isin(squids.Host) )
        awdata = add_institutions( awdata, geoip.get_isp)
        offending, totals_high = excess_failover_check( awdata, squids, site_rate_threshold)
    else:
        offending = None
        totals_high = None

    #TODO: Implement exception for some French machines
    if offending:
        get_sites = lambda inst: sites_from_institution(inst, geo)
        offending['Sites'] = offending['Institution'].apply(get_sites)

    gen_report (offending, groupname, geo, totals_high)
    failovers = update_record (offending, past_records, now, squids)

    return failovers

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
    cols = ['Hits', 'Bandwidth']

    # Get the deltas and rates of Hits and Bandwidth of recently updated/added hosts
    deltas = now_stats[cols] - last_stats[cols]
    deltas[~deltas.index.isin(last_stats.index)] = now_stats[cols]
    rates = deltas.copy() / float(delta_t)

    # Add computed columns to table, dropping the original columns since they
    # are a long-running accumulation.
    rates.rename(columns = lambda x: x + "Rate", inplace=True)
    table = now_stats.drop(cols, axis=1)\
                     .join([deltas, rates])\
                     .dropna()                  # Drop hosts that were not updated

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

    totals = by_institution[['HitsRate', 'BandwidthRate']].sum()
    totals_high = totals[ totals['HitsRate'] > site_rate_threshold ]

    offending = awdata[ awdata['Institution'].isin(totals_high.index) ].reset_index()
    squid_alias_map = squids.set_index('Host')['Alias']
    offending['Host'][offending['IsSquid']] = offending['Host'][offending['IsSquid']].map(squid_alias_map)

    return offending, totals_high

def gen_report (offending, groupname, geo, totals_high):

    print "Failover activity to %s:" % groupname

    if offending is None:
        print " None.\n"
        return

    for_report = offending.copy()

    if len(for_report) > 0:

        pd.options.display.width = 130
        pd.options.display.max_rows = 100
        print for_report.set_index(['Institution', 'IsSquid', 'Host']).sortlevel(0), "\n"

    else:
        print " None.\n"

def update_record (offending, past_records, now, squids):

    now_secs = datetime_to_UTC_epoch(now)

    if offending is None:
        return

    to_add = offending.drop('Institution', axis=1)
    to_add['Timestamp'] = now_secs
    to_add['IsSquid'] = to_add['Host'].isin(squids.Host)

    if past_records:
        update = pd.concat([past_records, to_add])
    else:
        update = to_add

    update['Bandwidth'] = update['Bandwidth'].astype(int)
    update['Hits'] = update['Hits'].astype(int)

    return update

def sites_from_institution (institution, geo):

    geolist_name_func = lambda s: s.encode('utf-8', 'ignore').replace(' ', '')

    sites = geo[ geo['Institution'] == geolist_name_func(institution) ]['Site'].unique().tolist()

    if not sites:
        site_list = institution
    else:
        site_list = ', '.join(sites)

    return site_list

if __name__ == "__main__":
    sys.exit(main())
