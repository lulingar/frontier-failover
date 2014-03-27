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

    if len(failover_groups):
        failover_record = pd.concat(failover_groups, ignore_index=True)
        write_failover_record(failover_record, config['record_file'])

        print "Sites to send alarm to:\n", mark_activity_for_mail(failover_record)

    return 0

def load_records (record_file, now, record_span):

    now_secs = datetime_to_UTC_epoch(now)
    old_cutoff = now_secs - record_span*3600

    if os.path.exists(record_file):
        records = pd.read_csv(record_file, index_col=False)
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

    instances = groupconf['awstats']
    last_stats_file = groupconf['file_last_stats']
    site_rate_threshold = groupconf['rate_threshold']        # Unit: Queries/sec

    awdata = fl.download_aggregated_awstats_data(instances)
    last_timestamp, last_awdata = load_last_data(last_stats_file)
    save_last_data(last_stats_file, awdata, now)

    if last_awdata is None:
        return None

    awdata = compute_traffic_delta( awdata, last_awdata, now, last_timestamp)

    if len(awdata) > 0:

        awdata['IsSquid'] = awdata['Ip'].isin(geo['Ip'])
        awdata = tagger_object.tag_hosts(awdata, 'Ip')
        offending = excess_failover_check(awdata, site_rate_threshold)

        """
        squid_alias_map = fl.get_squid_host_alias_map(geo)
        offending['Alias'] = ''
        offending['Alias'][offending['IsSquid']] = offending['Host'][offending['IsSquid']].map(squid_alias_map)
        """

    else:
        offending = None

    failovers = update_record (offending, past_records, now, geo)
    gen_report (offending, groupconf['name'], geo)

    return failovers

def load_last_data (last_stats_file):

    if os.path.exists(last_stats_file):
        fobj = open(last_stats_file)
        last_timestamp = datetime.utcfromtimestamp( int( fobj.next().strip()))
        fobj.close()

        last_awdata = pd.read_csv(last_stats_file, index_col=False, skiprows=1)
        last_awdata['Ip'] = last_awdata['Host'].apply(fl.get_host_ipv4_addr)

    else:
        last_awdata = None
        last_timestamp = None

    return last_timestamp, last_awdata

def save_last_data (last_stats_file, data, timestamp):

    fobj = open(last_stats_file, 'w')
    fobj.write( str(datetime_to_UTC_epoch(timestamp)) + '\n' )
    data.to_csv(fobj, index=False)
    fobj.close()

def datetime_to_UTC_epoch (dt):

    return calendar.timegm( dt.utctimetuple())

def compute_traffic_delta (now_stats_indexless, last_stats_indexless, now_timestamp, last_timestamp):

    cols = ['Hits', 'Bandwidth']

    now_stats = now_stats_indexless.set_index('Ip')
    last_stats = last_stats_indexless.set_index('Ip')

    # Get the deltas and rates of Hits and Bandwidth of recently updated/added hosts
    deltas = pd.np.subtract( *now_stats[cols].align(last_stats[cols], join='left') )
    # The deltas of newly recorded hosts are their very data values
    deltas.fillna( now_stats[cols], inplace=True )
    # Filter out hosts whose recent activity is null
    deltas = deltas[ deltas['Hits'] > 0 ]

    # Add computed columns to table, dropping the original columns since they
    # are a long-running accumulation.
    delta_t = datetime_to_UTC_epoch(now_timestamp) - datetime_to_UTC_epoch(last_timestamp)
    rates = deltas.copy() / float(delta_t)
    rates.rename(columns = lambda x: x + "Rate", inplace=True)
    table = now_stats.drop(cols, axis=1)\
                     .join([deltas, rates], how='inner')
    table.reset_index(inplace=True)

    return table

def excess_failover_check (awdata, site_rate_threshold):

    non_squid_stats = awdata[ ~awdata['IsSquid'] ].copy()
    by_sites = non_squid_stats.groupby('Sites')

    totals = by_sites['HitsRate'].sum()
    totals_high = totals[ totals > site_rate_threshold ]

    offending = awdata[ awdata['Sites'].isin(totals_high.index) ]

    return offending.copy()

def gen_report (offending, groupname, geo):

    print "Failover activity to %s:" % groupname,

    if offending is None:
        print " None.\n"
        return

    for_report = offending.copy()

    if len(for_report) > 0:

        pd.options.display.precision = 2
        pd.options.display.width = 130
        pd.options.display.max_rows = 100

        print '\n', for_report.set_index(['Sites', 'IsSquid', 'Host']).sortlevel(0), "\n"

    else:
        print " None.\n"

def update_record (offending, past_records, now, geo):

    now_secs = datetime_to_UTC_epoch(now)

    if offending is None:
        return

    to_add = offending.copy()
    to_add['Timestamp'] = int(now_secs)
    to_add['IsSquid'] = to_add['Ip'].isin(geo['Ip'])

    if isinstance(past_records, pd.DataFrame):
        update = pd.concat([past_records, to_add], ignore_index=True)
    else:
        update = to_add

    return update

def write_failover_record (record, file_path):

    column_order = ["Timestamp", "Group", "Sites", "Host", "Alias", "IsSquid",
                    "Bandwidth", "BandwidthRate", "Hits", "HitsRate"]

    failover_record = record.drop('Ip', axis=1)
    failover_record['Bandwidth'] = failover_record['Bandwidth'].astype(int)
    failover_record['Hits'] = failover_record['Hits'].astype(int)
    failover_record['Timestamp'] = failover_record['Timestamp'].astype(int)
    failover_record.reindex(columns=column_order, inplace=True)

    reduced_file_parts = file_path.split('.')
    reduced_file_parts.insert(len(reduced_file_parts)-1, 'reduced')
    reduced_file_path = '.'.join(reduced_file_parts)

    grouping = ['Group', 'Sites', 'IsSquid']
    field_ops = dict( (field, pd.np.sum) for field in
                       ('Hits', 'HitsRate', 'Bandwidth', 'BandwidthRate') )

    reduced_stats = failover_record.groupby(grouping, group_keys=False)\
                                   .apply(reduce_to_rank, columns='HitsRate',
                                          ranks=12, reduction_ops=field_ops,
                                          tagged_fields=['Host', 'Alias'])

    failover_record.to_csv(file_path, index=False, float_format="%.2f")
    reduced_stats.to_csv(reduced_file_path, index=False, float_format="%.2f")

def reduce_to_rank (dataframe, columns, ranks=5, reduction_ops={}, tagged_fields=[]):

    if len(dataframe) <= ranks:
        return dataframe

    df = dataframe.sort_index(by=columns, ascending=False)

    all_reduction_ops = dict( (field, pd.np.max) for field in df.columns )
    all_reduction_ops.update(reduction_ops)

    reduced = df[:ranks].T.copy()
    out_of_rank = df[ranks:]
    to_add = pd.Series( dict( (field, func(out_of_rank[field])) for field, func in
                         all_reduction_ops.items() ))
    if len(tagged_fields):
        to_add[tagged_fields] = 'Others'
    reduced['Others'] = to_add

    return reduced.T.copy()

def mark_activity_for_mail (records):

    x = records[['Sites', 'Timestamp']].drop_duplicates()
    # Wait is the time (in hours) elapsed between failover event records
    x['Wait'] = x.groupby('Sites')['Timestamp'].diff().fillna(0)/3600.0
    x.Wait = x.Wait.round().astype(int) - 1
    x.Timestamp = x.Timestamp.apply(pd.to_datetime, unit='s')

    # persistent failover is that which has no wait (i.e. happens continuously)
    persistent = x[x.Wait == 0]
    to_report = persistent.Sites.unique()

    return to_report

if __name__ == "__main__":
    sys.exit(main())
