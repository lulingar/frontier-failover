#!/usr/bin/env python

import calendar
import collections
import getpass
import json
import numbers
import os
import smtplib
import socket
import sys
import time
import types
import urllib

from datetime import datetime

import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd

import FailoverLib as fl

def main():

    pd.options.display.width = 180

    my_path = os.path.dirname(os.path.abspath(__file__))
    os.chdir(my_path)

    config_file = os.path.expanduser(sys.argv[1])
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
        issue_emails(failover_record, config, now_timestamp)

    return 0

def load_records (record_file, now_timestamp, record_span):

    old_cutoff = now_timestamp - record_span*3600
    file_path = os.path.expanduser(record_file)

    if os.path.exists(file_path):
        records = pd.read_csv(file_path, index_col=False)
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
        tagged_data = tagger_object.tag_hosts(awdata, 'Ip')
        offending = excess_failover_check(tagged_data, site_rate_threshold)
    else:
        offending = None

    failovers = update_record(offending, past_records, now_timestamp)
    gen_report(offending, groupconf['name'])

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
    fobj.write( '{0:d}\n'.format(timestamp) )
    data.to_csv(fobj, index=False)
    fobj.close()

def datetime_to_UTC_epoch (dt):

    return int(calendar.timegm( dt.utctimetuple()))

def compute_traffic_delta (now_stats_indexless, last_stats_indexless, now_timestamp, last_timestamp):

    cols = ['Hits', 'Bandwidth']

    now_stats = now_stats_indexless.set_index('Ip')
    last_stats = last_stats_indexless.set_index('Ip')

    # Get the deltas and rates of Hits and Bandwidth of recently updated/added hosts
    now_aligned, last_aligned = now_stats[cols].align(last_stats[cols], join='left')
    # Set previously unrecorded hosts as zero traffic
    deltas = now_aligned - last_aligned.fillna(0)
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

        to_print = for_report.set_index(['Sites', 'IsSquid', 'Host']).sortlevel(0)
        column_order = ['Hits', 'HitsRate', 'Bandwidth', 'BandwidthRate']
        print "\n" + to_print.reindex(columns=column_order).to_string() + "\n"

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

def mark_activity_for_mail (records, now_timestamp, window=None):

    if isinstance(window, numbers.Number):
        start_of_window = now_timestamp - window*3600
        view = records[ records.Timestamp >= start_of_window ]
    else:
        view = records

    x = view[['Sites', 'Timestamp']].drop_duplicates()
    # Wait is the time (in hours) elapsed between failover event records
    x['Wait'] = x.groupby('Sites')['Timestamp'].diff().fillna(0)/3600.0
    x.Wait = x.Wait.round().astype(int) - 1

    # persistent failover is that which has no wait (i.e. happens continuously)
    persistent = x[x.Wait == 0]
    to_report = persistent.Sites.unique()

    sys.stderr.write("Wait times in hours:\n" + x.to_string() + "\n")

    return to_report

def issue_emails (records, config, now_timestamp):

    marked = mark_activity_for_mail(records, now_timestamp,
                                    window=config['emails']['periodicity'])
    if not len(marked):
        return

    sites_delimiter = '; '

    template_file = os.path.expanduser(config['emails']['template_file'])
    template = open(template_file).read()
    contacts_file = os.path.expanduser(config['emails']['list_file'])
    contacts = fl.parse_site_contacts_file(contacts_file)
    mailing_list = config['emails']['support_list']
    operator_email = config['emails']['operator_email']
    alarm_list = config['emails']['alarm_list']
    emails_records = load_records(config['emails']['record_file'], now_timestamp,
                                  config['history']['span'])

    format_floats = lambda f: unicode("{0:.2f}".format(f))
    rate_col_name = "RateThreshold [*]"
    groupcode_name = "GCode"
    groups = []
    for group_key, group_info in config['groups'].items():
        groups.append({"Group": group_info['name'],
                       rate_col_name: group_info['rate_threshold'],
                       groupcode_name: group_key})
    column_order = ['Group', groupcode_name, rate_col_name]
    groups_df = pd.DataFrame.from_records(groups, columns=column_order)

    sent_emails = []
    if isinstance(emails_records, pd.DataFrame):
        start_of_window = now_timestamp - config['emails']['periodicity']*3600
        recent_notifications = emails_records[ emails_records.Timestamp >= start_of_window ]
        new_sites = set(marked) - set(recent_notifications.Sites.tolist())
    else:
        new_sites = marked

    print "Sites to send alarm to:\n", new_sites

    for sites in new_sites:

        site_list = sites.split(sites_delimiter)
        from_site = records[records.Sites == sites]
        latest_timestamp = from_site.Timestamp.max()
        view = from_site[from_site.Timestamp == latest_timestamp]\
                        .drop(['Sites', 'Timestamp'], axis=1)\
                        .rename(columns={'Group': groupcode_name})
        table = view.set_index(['IsSquid', groupcode_name, 'Host'])\
                    .sortlevel(0, ascending=False)\
                    .reindex(columns=['Ip', 'Hits', 'Bandwidth'])
        table.Bandwidth = table.Bandwidth.apply(fl.from_bytes)

        aggregation = view.groupby(['IsSquid', groupcode_name])['Hits', 'Bandwidth'].sum()\
                          .sortlevel(0, ascending=False)
        aggregation.Bandwidth = aggregation.Bandwidth.apply(fl.from_bytes)

        if any( site in contacts for site in site_list ):
            target_emails = fl.flatten( contacts[site] for site in site_list )
        else:
            target_emails = [ operator_email ]

        site_filter = encodeURIComponent(sites.replace(sites_delimiter, '\n'))
        message_str = template.format(record_span=config['history']['span'],
                                      site_query_url=site_filter,
                                      support_email=mailing_list,
                                      base_url=config['base_url'],
                                      site_name=sites,
                                      server_groups=groups_df.to_string(index=False,
                                                                        float_format=format_floats,
                                                                        justify='right'),
                                      summary_table=table.to_string(justify='right'),
                                      aggregated_table=aggregation.to_string(justify='right'),
                                      period=config['history']['period'])

        print "\nAbout to send email about", sites, "to:", email.utils.COMMASPACE.join(target_emails)

        successful = send_email("Direct Connections to Frontier servers from " + sites,
                                message_str,
                                to=target_emails,
                                cc=alarm_list,
                                reply_to=mailing_list)

        if successful:
            sent_emails.append({'Timestamp': now_timestamp, 'Sites': sites,
                                'Addresses': ', '.join(target_emails)})
            time.sleep(1)

    new_df = pd.DataFrame.from_records(sent_emails)
    if isinstance(emails_records, pd.DataFrame):
        new_df = pd.concat([emails_records, new_df])
    new_df.Timestamp = new_df.Timestamp.astype(int)

    new_df.to_csv(os.path.expanduser(config['emails']['record_file']), index=False)

    return

def send_email (subject, message, to, reply_to='', cc='', html_message=''):

    #user, host = get_user_and_host_names()
    user, host = "squidmon", "mail.cern.ch"
    sender = '%s@%s' % (user, host)

    receivers = make_address_list(to)
    copies = make_address_list(cc)

    msg = MIMEMultipart('alternative')

    msg['Subject'] = subject
    #msg['From'] = sender
    msg['To'] = email.utils.COMMASPACE.join(receivers)
    if reply_to:
        msg.add_header('Reply-to', reply_to)
    if len(copies):
        msg.add_header('CC', email.utils.COMMASPACE.join(copies))

    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    if message:
        msg.attach( MIMEText(message, 'plain'))
    if html_message:
        msg.attach( MIMEText(html_message, 'html'))

    try:
        smtpObj = smtplib.SMTP('localhost')
        smtpObj.set_debuglevel(True)
        smtpObj.sendmail(sender, receivers + copies, msg.as_string())
        smtpObj.quit()
        print "\nSuccessfully sent email to:", msg['To']
        print " CC'ed to:", msg['CC']
        return True
    except:
        return False

def get_user_and_host_names():

    user = getpass.getuser()
    host = socket.gethostname()

    return user, host

def make_address_list (addresses):

    if isinstance(addresses, types.StringTypes):
        receivers = addresses.replace(' ','').split(',')

    elif isinstance(addresses, collections.Iterable):
        receivers = addresses
    else:
        raise TypeError("Bad Addresses list:" + repr(addresses))

    address_list = list(set(receivers))

    return address_list

def encodeURIComponent(to_encode):

    encoded = unicode(to_encode).encode('utf-8')

    return urllib.quote(encoded, safe='~()*!.\'')

if __name__ == "__main__":
    sys.exit(main())
