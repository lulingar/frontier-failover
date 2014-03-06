#! /usr/bin/env python

import re
import socket
import urllib2

import pandas as pd
import pygeoip

from datetime import datetime

def to_bytes (sz):

    factors = {'bytes': 0, 'kb': 10, 'mb': 20, 'gb': 30, 'tb': 40}

    if sz == '0': return 0

    value, fac = sz.strip().split()
    factor = factors[ fac.lower() ]

    return int( round( float(value) * (2 ** factor) ))

def load_awstats_data (machine, day=None):

    server = "http://frontier.cern.ch/"
    url = "awstats/awstats.pl?config={instance}&databasebreak=day&day={day:d}&framename=mainright&output=allhosts"

    if not day:
        day = datetime.today().day

    aw_url = server + url.format(instance=machine, day=day)
    dataframe = pd.read_html( aw_url, header = 0,
                              attrs = {'class': 'aws_data'},
                              infer_types = False )[1]

    stats = dataframe.columns[0]
    dataframe.rename(columns = {stats: 'Host'}, inplace=True)

    del dataframe['Pages'] # Because it is identical to 'Hits'

    dataframe['Hits'] = dataframe['Hits'].map(int)
    dataframe['Bandwidth'] = dataframe['Bandwidth'].map(to_bytes).astype(int)
    dataframe['Last visit'] = parse_timestamp_column(dataframe['Last visit'])

    return dataframe

def load_aggregated_awstats_data (machines, day=None):

    data = {}
    for machine in machines:
        data[machine] = load_awstats_data(machine, day).set_index('Host')

    panel = pd.Panel(data)
    frame = panel.transpose(minor='items', items='minor', major='major')\
                 .to_frame(filter_observations=True)
    aggregated = frame.groupby(level=0).agg( {'Last visit': max,
                                              'Hits': sum,
                                              'Bandwidth': sum})
    aggregated.index.name = 'Host'

    return aggregated

def get_url (url):

    rq = urllib2.Request (url)
    data = urllib2.urlopen(rq).read()

    return data

def parse_geolist (geolistdata):

    geo_str = unicode (geolistdata, errors='ignore').encode('utf-8')
    lines = geo_str.replace('"','').split()

    step1 = ' '.join(lines).split('Directory')
    step2 = ( line.split() for line in step1 )
    step3 = filter (lambda e: len(e) == 6, step2)

    squids = []

    for e in step3:
        institution = re.sub('[/<>]', '', e[0])
        site = e[2]
        proxies = set( e[4].strip(';DIRECT').replace(';','|').split('|') )

        for proxy in proxies:
            host_name = proxy.replace('/','')
            squids.extend( gen_geo_entry(host_name, institution, site) )

    return pd.DataFrame(squids)

def gen_geo_entry (squid_hostname, institution, site):

    host_data = squid_hostname.split(':')
    if len(host_data) == 1:
        listed_host_name = host_data[0]
        protocol = port = ''
    else:
        protocol, listed_host_name, port = host_data

    ip_addresses = simple_get_host_ipv4 (listed_host_name)
    is_dns = (len(ip_addresses) > 1)

    entries = []
    for ip in ip_addresses:

        if ip == '0.0.0.0':
            host_name = listed_host_name
        else:
            host_name = socket.getfqdn(ip)

        entries.append({'Institution': institution,
                        'Site': site,
                        'Host': host_name,
                        'Alias': listed_host_name,
                        'Ip': ip,
                        'Port': port,
                        'IsDNS': is_dns})
    return entries

def parse_exceptionlist (exceptionlist_data):

    site_action = []
    site_WorkerNode_view = []
    site_monitoring_view = []

    for line in exceptionlist_data.splitlines():

        if not line or line[0] == '#':
            continue

        elements = line.split()

        site = elements[0].strip('+-')
        action = elements[0][0]

        hosts = elements[1:]
        monitoring_hosts = [ h[1:] for h in hosts if h[0] == '+' ]
        workernode_hosts = [ h[1:] for h in hosts if h[0] == '-' ]

        if len(hosts) != (len(monitoring_hosts) + len(workernode_hosts)):
            message = "Incomplete host specification for site {0} (ignored)".format(site)
            print message
            print "  >>", line
            continue

        for host in monitoring_hosts:
            site_monitoring_view.append({'Site': site, 'Host': host})

        for host in workernode_hosts:
            site_WorkerNode_view.append({'Site': site, 'Host': host})

        if action in '+-':
            site_action.append({'Site': site, 'Action': action})

    actions = pd.DataFrame(site_action).set_index('Site')['Action']
    workernode_view = pd.DataFrame(site_WorkerNode_view)
    monitoring_view = pd.DataFrame(site_monitoring_view)

    return actions, workernode_view, monitoring_view

def patch_geo_table (geo, MO_view, actions, geoip):

    MO_eview = MO_view.copy()
    MO_eview['Base'], MO_eview['Spec'] = zip(*MO_eview['Site'].map(site_name_split))
    MO_eview = MO_eview.merge(actions.reset_index(), how='left', on='Site')\
                       .fillna('')\
                       .drop('Site', axis='columns')\
                       .rename(columns={'Base': 'Site'})\
                       .sort('Site')
    MO_eview['Institution'] = MO_eview['Host'].apply(geoip.get_isp)
    MO_eview = MO_eview[ MO_eview.Action != '-' ]
    to_add = MO_eview[ ~(MO_eview.Host.isin(geo.Host) | MO_eview.Host.isin(geo.Alias)) ]
    to_add.drop(['Action', 'Spec'], axis='columns', inplace=True)
    new_geo_entries = [ gen_geo_entry(spec['Host'], spec['Institution'], spec['Site'])
                       for spec in to_add.to_dict('records') ]
    geo_app = pd.DataFrame( sum(new_geo_entries, []) )
    new_geo = pd.concat([geo, geo_app])

    return new_geo

def site_name_split (site_name):

    parts = site_name.split('_', 3)
    base = '_'.join(parts[:3])
    if len(parts) == 3:
        extra = ''
    else:
        if len(parts[3]) > 2:
            extra = ''
            base = site_name
        else:
            extra = parts[3]

    return base, extra

def build_squids_list (geo_table):

    non_dns = geo_table[~geo_table.IsDNS].Host
    squid_names = set( non_dns.tolist() )

    """
    Build Host->Alias mapping, with cases where the alias differs
    overruling cases where the alias is the same as the host name.
    """
    mapping = {}
    for alias in squid_names:
        host = socket.getfqdn(alias)
        if host not in mapping:
            mapping[host] = ''
        if alias != host:
            mapping[host] = alias

    squids = pd.DataFrame(mapping.items(), columns=('Host', 'Alias'))

    return squids

def simple_get_host_ipv4 (hostname):

    if is_a_valid_ip (hostname):
        return [hostname]

    try:
        ip = list (get_dns_addresses (hostname))

    except (socket.error, socket.herror):
        ip = ['0.0.0.0']

    return ip

def is_a_valid_ip (address):

    parts = address.split('.')
    try:
        return len(parts) == 4 and all( (0 <= int(x) < 256) for x in parts )
    except:
        return False

def get_dns_addresses (hostname):

    # AF_INET restricts results to IPv4
    info = socket.getaddrinfo( hostname, 0, socket.AF_INET)
    return set( e[4][0] for e in info )

class GeoIPWrapper(object):

    def __init__ (self, geoip_db_file):

        self.geoip = pygeoip.GeoIP(geoip_db_file)

    def get_isp (self, host):

        try:
            isp = self.geoip.org_by_name(host)\
                      .encode('ascii', 'ignore')\
                      .replace(' ', '')  # Spaces are removed in the geolist

        except (socket.gaierror, AttributeError):
            isp = 'Unknown'

        return isp

def parse_timestamp_column (series):

    epoch_ns = pd.to_datetime(series) - datetime(1970,1,1)
    epoch = epoch_ns.astype(int) / int(1e9)

    return epoch

if __name__ == "__main__":

    server_lists = "http://wlcg-squid-monitor.cern.ch/"
    geolist_file = server_lists + "geolist.txt"
    exception_list_file = server_lists + "exceptionlist.txt"

    geo = parse_geolist( get_url( geolist_file))
    actions, WN_view, MO_view = parse_exceptionlist( get_url( exception_list_file))
    squids = build_squids_list(geo, MO_view)

    print squids

