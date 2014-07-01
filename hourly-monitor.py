#!/usr/bin/env python

import calendar
import getpass
import json
import os
import smtplib
import socket
import sys
import urllib

from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd

import FailoverLib as fl

"""
TODO
     See comments in the code for TODO items
"""
def main():

    pd.options.display.width = 180

    my_path = os.path.dirname(os.path.abspath(__file__))
    os.chdir(my_path)

    config_file = sys.argv[1]
    config = json.load( open(config_file))
    json.dump(config, open(config_file, 'w'), indent=3)

    geoip_database_file = os.path.expanduser(config['geoip_db'])
    geoip = fl.GeoIPWrapper(geoip_database_file)

    exception_list = fl.get_url(config['exception_list'])
    actions, WN_view, MO_view = fl.parse_exceptionlist(exception_list)

    geo_list = fl.get_url(config['geo_list'])
    geo_0 = fl.parse_geolist(geo_list)

    geo = fl.patch_geo_table(geo_0, MO_view, WN_view, actions, geoip)
    cms_tagger = fl.CMSTagger(geo, geoip)

    now_timestamp = datetime_to_UTC_epoch( datetime.utcnow() )
    current_records = load_records(config['record_file'], now_timestamp, config['history']['span'])
    failover_groups = []

    for machine_group_name in config['groups'].keys():

        past_records = get_group_records(current_records, machine_group_name)
        failover = analyze_failovers_to_group( config, machine_group_name,
                                               now_timestamp, past_records,
                                               geo, cms_tagger )
        if isinstance(failover, pd.DataFrame):
            failover['Group'] = machine_group_name
            failover_groups.append(failover.copy())

    if len(failover_groups):
        failover_record = pd.concat(failover_groups, ignore_index=True)
        write_failover_record(failover_record, config['record_file'])

        marked = mark_activity_for_mail(failover_record)
        if len(marked):
            issue_emails(failover_record, marked, config, now_timestamp)

    return 0

def load_records (record_file, now_timestamp, record_span):

    old_cutoff = now_timestamp - record_span*3600

    if os.path.exists(record_file):
        records = pd.read_csv(record_file, index_col=False)
        records = records[ records['Timestamp'] >= old_cutoff ]
    else:
        records = None

    return records

def get_group_records (records, group_name):

    if isinstance(records, pd.DataFrame) and 'Group' in records:
        return records[ records['Group'] == group_name ].copy()
    else:
        return None

def analyze_failovers_to_group (config, groupname, now_timestamp, past_records, geo, tagger_object):

    groupconf = config['groups'][groupname]

    instances = groupconf['awstats']
    base_path = groupconf['awstats_base']
    last_stats_file = groupconf['file_last_stats']
    site_rate_threshold = groupconf['rate_threshold']        # Unit: Queries/sec

    awdata = fl.download_aggregated_awstats_data(instances, base_path)
    last_timestamp, last_awdata = load_last_data(last_stats_file)
    save_last_data(last_stats_file, awdata, now_timestamp)

    if last_awdata is None:
        return None

    awdata = compute_traffic_delta( awdata, last_awdata, now_timestamp, last_timestamp)

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

    failovers = update_record (offending, past_records, now_timestamp)
    gen_report (offending, groupconf['name'])

    return failovers

def load_last_data (last_stats_file):

    if os.path.exists(last_stats_file):
        fobj = open(last_stats_file)
        last_timestamp = int( fobj.next().strip())
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

    return int(calendar.timegm( dt.utctimetuple()))

def compute_traffic_delta (now_stats_indexless, last_stats_indexless, now_timestamp, last_timestamp):

    cols = ['Hits', 'Bandwidth']

    now_stats = now_stats_indexless.set_index('Ip')
    last_stats = last_stats_indexless.set_index('Ip')

    # Get the deltas and rates of Hits and Bandwidth of recently updated/added hosts
    deltas = pd.np.subtract( *now_stats[cols].align(last_stats[cols], join='left') )
    # The deltas of newly recorded hosts are their very data values
    deltas.fillna( now_stats[cols], inplace=True )
    # Filter out hosts whose recent activity is null
    are_active = (deltas['Hits'] > 0) & (deltas['Bandwidth'] > 0)
    active = deltas[are_active].copy()

    inactive = deltas[~are_active].copy()
    inactive['WasOld'] = inactive.index
    inactive['WasOld'] = inactive['WasOld'].isin(last_stats.index)

    # Add computed columns to table, dropping the original columns since they
    # are an accumulation.
    delta_t = float(now_timestamp - last_timestamp)
    rates = active / delta_t
    rates = rates.rename(columns = lambda x: x + "Rate")
    table = now_stats.drop(cols, axis=1)\
                     .join([active, rates], how='inner')
    table.reset_index(inplace=True)

    return table

def excess_failover_check (awdata, site_rate_threshold):

    non_squid_stats = awdata[ ~awdata['IsSquid'] ].copy()
    by_sites = non_squid_stats.groupby('Sites')

    totals = by_sites['HitsRate'].sum()
    totals_high = totals[ totals > site_rate_threshold ]

    offending = awdata[ awdata['Sites'].isin(totals_high.index) ]

    return offending.copy()

def gen_report (offending, groupname):

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

def update_record (offending, past_records, now_timestamp):

    to_concat = []

    if isinstance(past_records, pd.DataFrame):
        to_concat.append(past_records)

    if isinstance(offending, pd.DataFrame):
        new_records = offending.copy()
        new_records['Timestamp'] = int(now_timestamp)
        to_concat.append(new_records)

    updated = pd.concat(to_concat, ignore_index=True) if to_concat else None

    return updated

def write_failover_record (record, file_path):

    column_order = ["Timestamp", "Group", "Sites", "Host", "Ip", "Alias", "IsSquid",
                    "Bandwidth", "BandwidthRate", "Hits", "HitsRate"]

    failover_record = record.reindex(columns=column_order)
    failover_record['Bandwidth'] = failover_record['Bandwidth'].astype(int)
    failover_record['Hits'] = failover_record['Hits'].astype(int)
    failover_record['Timestamp'] = failover_record['Timestamp'].astype(int)

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

    # persistent failover is that which has no wait (i.e. happens continuously)
    persistent = x[x.Wait == 0]
    to_report = persistent.Sites.unique()

    return to_report

def issue_emails (records, marked_sites, config, now_timestamp):

    print "Sites to send alarm to:\n", marked_sites

    template_file = "failover-email.plain.tpl"
    mailing_list = "cms-frontier-support@cern.ch"
    template = open(template_file).read()

    format_floats = lambda f: unicode("{0:.2f}".format(f))
    rate_col_name = "RateThreshold[*]"

    for site in marked_sites:

        table = records[(records.Sites == site) & (records.Timestamp == now_timestamp)]\
                       .drop(['Sites', 'Timestamp'], axis=1)\
                       .set_index(['IsSquid', 'Group', 'Host'])\
                       .sortlevel(0)\
                       .reindex(columns=['Ip', 'Hits', 'Bandwidth'])
        table.Bandwidth = table.Bandwidth.apply(fl.from_bytes)

        groups = []
        for group_key, group_info in config['groups'].items():
            groups.append({"Group": group_info['name'], rate_col_name: group_info['rate_threshold'], "Code": group_key})

        groups_df = pd.DataFrame.from_records(groups, columns=['Group', 'Code', rate_col_name])

        message_str = template.format(record_span=config['history']['span'],
                                      site_query_url=encodeURIComponent(site.replace('; ', '\n')),
                                      support_mailing_list=mailing_list,
                                      site_name=site,
                                      server_groups=groups_df.to_string(index=False, float_format=format_floats, justify='right'),
                                      summary_table=table.to_string(float_format=format_floats, justify='right'),
                                      period=config['history']['period'])

        send_email("Direct Connections to Frontier servers from " + site,
                   message_str,
                   to="luis.linares@cern.ch",
                   reply_to=mailing_list)

def send_email (subject, message, to, reply_to='', html_message=''):

    user, host = get_user_and_host_names()
    sender = '%s@%s' % (user, host)

    if isinstance(to, str):
        receivers = to.split(',')
    elif isinstance(to, list):
        receivers = to

    msg = MIMEMultipart('alternative')

    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ", ".join(receivers)
    if reply_to:
        msg.add_header('Reply-to', reply_to)

    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    if message:
        msg.attach( MIMEText(message, 'plain'))
    if html_message:
        msg.attach( MIMEText(html_message, 'html'))

    try:
       smtpObj = smtplib.SMTP('localhost')
       smtpObj.sendmail (sender, receivers, msg.as_string())
       print "Successfully sent email to:", ', '.join(receivers)

    except smtplib.SMTPException:
       print "Error: unable to send email"

def get_user_and_host_names():

    user = getpass.getuser()
    host = socket.gethostname()

    return user, host

def encodeURIComponent(to_encode):
    encoded = unicode(to_encode).encode('utf-8')

    return urllib.quote(encoded, safe='~()*!.\'')

if __name__ == "__main__":
    sys.exit(main())
