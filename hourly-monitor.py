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
     See comments in the code for TODO items
"""
def main():

    my_path = os.path.dirname(os.path.abspath(__file__))
    geoip_database_file = "~/scripts/geolist/GeoIPOrg.dat"
    config_file = os.path.join(my_path, "instance_config.json")
    server_lists = "http://wlcg-squid-monitor.cern.ch/"
    geolist_file = server_lists + "geolist.txt"
    exception_list_file = server_lists + "exceptionlist.txt"

    print_column_order = ('Timestamp','Sites','Group','IsSquid','Host','Alias',
                          'Hits','HitsRate','Bandwidth','BandwidthRate',
                          'Last visit')

    config = json.load( open(config_file))
    json.dump(config, open(config_file, 'w'), indent=3)

    now = datetime.utcnow()
    geoip = fl.GeoIPWrapper( os.path.expanduser( geoip_database_file))

    actions, WN_view, MO_view = fl.parse_exceptionlist( fl.get_url( exception_list_file))
    geo_0 = fl.parse_geolist( fl.get_url( geolist_file))
    geo = fl.patch_geo_table(geo_0, MO_view, actions, geoip)
    cms_tagger = fl.CMSTagger(geo, geoip)

    current_records = load_records(config['record_file'], now, config['history']['span'])
    failover_groups = []

    for machine_group_name in config['groups'].keys():

        past_records = get_group_records(current_records, machine_group_name)
        failover = analyze_failovers_to_group( config, machine_group_name, now,
                                               past_records, geo, cms_tagger )
        if isinstance(failover, pd.DataFrame):
            failover['Group'] = machine_group_name
            failover_groups.append(failover.copy())

    failover_record = pd.concat(failover_groups, ignore_index=True)\
                        .reindex(columns=print_column_order)
    failover_record.to_csv(config['record_file'], index=False, float_format="%.2f")

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

    if isinstance(records, pd.DataFrame) and 'Group' in records:
        return records[ records['Group'] == group_name ]
    else:
        return None

def analyze_failovers_to_group (config, groupname, now, past_records, geo, tagger_object):

    groupconf = config['groups'][groupname]

    instances = groupconf['instances']
    last_stats_file = groupconf['file_last_stats']
    site_rate_threshold = groupconf['rate_threshold']   # Unit: Queries/sec

    awdata = fl.download_aggregated_awstats_data(instances)
    last_timestamp, last_awdata = load_last_data(last_stats_file)
    save_last_data(last_stats_file, awdata, now)

    if last_awdata is None:
        return None

    awdata = compute_traffic_delta( awdata, last_awdata, now, last_timestamp)

    if len(awdata) > 0:

        awdata.reset_index(inplace=True)
        awdata['Ip'] = awdata['Host'].apply(get_host_ipv4_addr)
        awdata = tagger_object.tag_hosts(awdata)

        squid_alias_map = fl.get_squid_host_alias_map(geo)
        awdata['Alias'] = ''
        awdata['Alias'][awdata['IsSquid']] = awdata['Host'][awdata['IsSquid']].map(squid_alias_map)

        offending, totals_high = excess_failover_check(awdata, site_rate_threshold)

    else:
        offending = None
        totals_high = None

    gen_report (offending, groupname, geo, totals_high)
    failovers = update_record (offending, past_records, now, geo)

    return failovers

def load_last_data (last_stats_file):

    if os.path.exists(last_stats_file):
        fobj = open(last_stats_file)
        last_timestamp = datetime.utcfromtimestamp( int( fobj.next().strip()))
        fobj.close()

        last_awdata = pd.read_csv(last_stats_file, skiprows=1, index_col=0)
        last_awdata['Ip'] = last_awdata['Host'].apply(get_host_ipv4_addr)

    else:
        last_awdata = None
        last_timestamp = None

    return last_timestamp, last_awdata

def save_last_data (last_stats_file, data, timestamp):

    fobj = open(last_stats_file, 'w')
    fobj.write( str(datetime_to_UTC_epoch(timestamp)) + '\n' )
    data.to_csv(fobj)
    fobj.close()

def get_host_ipv4_addr (host):

    return fl.simple_get_hosts_ipv4_addrs(host)[0]

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

def excess_failover_check (awdata, site_rate_threshold):

    non_squid_stats = awdata[ ~awdata['IsSquid'] ]
    by_sites = non_squid_stats.groupby('Sites')

    totals = by_sites[['HitsRate', 'BandwidthRate']].sum()
    totals_high = totals[ totals['HitsRate'] > site_rate_threshold ]

    offending = awdata[ awdata['Sites'].isin(totals_high.index) ]

    return offending, totals_high

def gen_report (offending, groupname, geo, totals_high):

    print "Failover activity to %s:" % groupname

    if offending is None:
        print " None.\n"
        return

    for_report = offending.copy()

    if len(for_report) > 0:

        pd.options.display.precision = 2
        pd.options.display.width = 130
        pd.options.display.max_rows = 100

        print for_report.set_index(['Sites', 'IsSquid', 'Host']).sortlevel(0), "\n"

    else:
        print " None.\n"

def update_record (offending, past_records, now, geo):

    now_secs = datetime_to_UTC_epoch(now)

    if offending is None:
        return

    to_add = offending.copy()
    to_add['Timestamp'] = now_secs
    to_add['IsSquid'] = to_add['Ip'].isin(geo['Ip'])

    if past_records:
        update = pd.concat([past_records, to_add], ignore_index=True)
    else:
        update = to_add

    update['Bandwidth'] = update['Bandwidth'].astype(int)
    update['Hits'] = update['Hits'].astype(int)
    update['Last visit'] = update['Last visit'].astype(int)

    return update

if __name__ == "__main__":
    sys.exit(main())
