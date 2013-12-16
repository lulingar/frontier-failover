#! /usr/bin/env python

import calendar
import json
import os
import sys

from datetime import datetime

import pandas as pd
pd.options.display.width = 130

import FailoverLib as fl

def main():

    #geoip_database_file = "~llinares/work/private/frontier/geoip/GeoIPOrg.dat"
    geoip_database_file = "~/scripts/geolist/GeoIPOrg.dat"
    groups_config_file = "instance_config.json"

    server_lists = "http://wlcg-squid-monitor.cern.ch/"
    geolist_file = server_lists + "geolist.txt"
    exception_list_file = server_lists + "exceptionlist.txt"

    geo = fl.parse_geolist( fl.get_url( geolist_file))
    actions, WN_view, MO_view = fl.parse_exceptionlist( fl.get_url( exception_list_file))
    squids = fl.build_squids_list(geo, MO_view)
    geoip = fl.GeoIPWrapper( os.path.expanduser( geoip_database_file))

    groups = json.load( open( groups_config_file))
    json.dump(groups, open(groups_config_file, 'w'), indent=3)

    for machine_group_name, machine_group_conf in groups.items():
        analyze_failovers_to_group( machine_group_name, machine_group_conf, geo, squids, geoip)

    return 0

def analyze_failovers_to_group (groupname, groupconf, geo, squids, geoip):

    instances = groupconf['instances']
    last_stats_file = groupconf['file_last_stats']
    site_rate_threshold = groupconf['rate_threshold']   # Unit: Queries/sec

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
        non_squid_stats, insti_high, offending = excess_failover_check( awdata, squids, site_rate_threshold)

    else:
        offending = None

    gen_report (groupname, offending, geo)

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

    table = now_stats.copy()
    delta_t = datetime_to_UTC_epoch(now_timestamp) - datetime_to_UTC_epoch(last_timestamp)
    delta_h = (now_stats['Hits'] - last_stats['Hits']).fillna(now_stats['Hits'])
    hits_column_idx = now_stats.columns.tolist().index('Hits')
    change = delta_h / float(delta_t)
    table.insert( hits_column_idx + 1, 'DHits_Dt', change)
    table = table.dropna()

    return table

def add_institutions (awstats_dataframe, isp_func):

    xaw = pd.DataFrame(awstats_dataframe.reset_index()['Host'])
    xaw['Institution'] = xaw.Host.apply(isp_func)
    xaw.set_index('Host', inplace=True)
    updated = pd.merge(awstats_dataframe, xaw, left_index=True, right_index=True)

    return updated

def excess_failover_check (awdata, squids, site_rate_threshold):

    non_squid_stats = awdata[ ~awdata.IsSquid ]

    by_inst = non_squid_stats.groupby('Institution')
    insti_traffic = by_inst[['DHits_Dt', 'Bandwidth']].sum()
    insti_high = insti_traffic[ insti_traffic.DHits_Dt > site_rate_threshold ]

    offending = awdata[ awdata.Institution.isin(insti_high.index) ].reset_index()
    squid_alias_map = squids.set_index('Host')['Alias']
    offending['Host'][offending.IsSquid] = offending['Host'][offending.IsSquid].map(squid_alias_map)

    return non_squid_stats, insti_high, offending

def gen_report (groupname, offending, geo):

    print "Failover activity to %s:" % groupname

    if offending is None:
        print " None.\n"
        return

    geolist_func = lambda s: s.encode(errors='ignore').replace(' ', '')
    get_sites = lambda inst:', '.join( geo[ geo.Institution ==
                                             geolist_func(inst)]['Site'].unique().tolist())
    for_report = offending.copy()
    for_report['Sites'] = for_report.Institution.apply(get_sites)
    for_report.set_index(['Institution', 'IsSquid', 'Host'], inplace=True)
    for_report.sortlevel(0, inplace=True)

    if len(for_report) > 0:
        print for_report, "\n"
    else:
        print " None.\n"

if __name__ == "__main__":
    sys.exit(main())
