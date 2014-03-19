#! /usr/bin/env python

import re
import socket
import urllib2

import pandas as pd
import pygeoip

from datetime import datetime
from functools import partial

def to_bytes (sz):

    factors = {'b': 0, 'bytes': 0, 'kb': 10, 'mb': 20, 'gb': 30, 'tb': 40}

    if sz == '0': return 0

    value, fac = sz.strip().split()
    factor = factors[ fac.lower() ]

    return int( round( float(value) * (2 ** factor) ))

def load_awstats_data (machine, day=None):

    server = "http://frontier.cern.ch/"
    url = ("awstats/awstats.pl?config={instance}&databasebreak=day"
           "&day={day:02d}&framename=mainright&output=allhosts")

    if not day:
        day = datetime.today().day

    aw_url = server + url.format(instance=machine, day=day)
    dataframe = pd.read_html( aw_url, header = 0,
                              attrs = {'class': 'aws_data'})[1]

    if isinstance(dataframe, pd.DataFrame):

        stats = dataframe.columns[0]
        dataframe.rename(columns = {stats: 'Host'}, inplace=True)

        del dataframe['Pages']               # Because it is identical to 'Hits'
        del dataframe['Last visit']        # Most of the time is not useful info

        dataframe['Hits'] = dataframe['Hits'].astype(int)
        dataframe['Bandwidth'] = dataframe['Bandwidth'].map(to_bytes).astype(int)

    else:
        dataframe = pd.DataFrame(None,
                                 columns=('Host', 'Hits', 'Bandwidth', 'Last visit'))

    return dataframe

def download_aggregated_awstats_data (machines, day=None):

    data = {}
    for machine in machines:
        data[machine] = load_awstats_data(machine, day).set_index('Host')

    panel = pd.Panel(data)
    frame = panel.transpose(minor='items', items='minor', major='major')\
                 .to_frame(filter_observations=True)
    aggregated = frame.groupby(level=0).agg( {'Hits': sum,
                                              'Bandwidth': sum})
    aggregated.index.name = 'Host'
    aggregated.reset_index(inplace=True)

    aggregated['Ip'] = aggregated['Host'].apply(get_host_ipv4_addr)

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
            squids.extend( gen_geo_entries(host_name, institution, site) )

    return pd.DataFrame(squids)

def gen_geo_entries (squid_hostname, institution, site):

    host_data = squid_hostname.split(':')
    if len(host_data) == 1:
        listed_host_name = host_data[0]
        protocol = port = ''
    else:
        protocol, listed_host_name, port = host_data

    ip_addresses = simple_get_hosts_ipv4_addrs(listed_host_name)
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
    MO_eview['Base'], MO_eview['Spec'] = zip(*MO_eview['Site'].map(cms_site_name_split))
    MO_eview = MO_eview.merge(actions.reset_index(), how='left', on='Site')\
                       .fillna('')\
                       .drop('Site', axis='columns')\
                       .rename(columns={'Base': 'Site'})
    MO_eview = MO_eview[ MO_eview.Action != '-' ]
    MO_eview['Institution'] = MO_eview['Host'].apply(geoip.org_by_name)
    to_add = MO_eview[ ~(MO_eview.Host.isin(geo.Host) | MO_eview.Host.isin(geo.Alias)) ].copy()
    to_add.drop(['Action', 'Spec'], axis='columns', inplace=True)
    new_geo_entries = [ gen_geo_entries(spec['Host'], spec['Institution'], spec['Site'])
                        for spec in to_add.to_dict('records') ]
    geo_app = pd.DataFrame( sum(new_geo_entries, []) )
    new_geo = pd.concat([geo, geo_app], ignore_index=True)

    return new_geo

def tag_hosts (dataframe, host_ip_field, squids_institute_sites_map, squids_ip_sites_map, geo, geoip):

    data = dataframe.copy()
    data['Sites'] = ''
    wn_fun = partial(assign_site_workernode, squids_inst_site_map=squids_institute_sites_map,
                                             geoip=geoip )
    #TODO: Implement IP exception for some French machines

    data['Sites'][data['IsSquid']] = data[host_ip_field][data['IsSquid']].map(squids_ip_sites_map)
    data['Sites'][~data['IsSquid']] = data[host_ip_field][~data['IsSquid']].apply(wn_fun)

    return data

# Get a Worker node's parent site list through GeoIP
def assign_site_workernode (host, squids_inst_site_map, geoip):

    if host in ('127.0.0.1', 'localhost', 'localhost6', '::1'):
        return 'localhost'

    institution = geoip.org_by_addr(host)

    try:
        site = squids_inst_site_map[institution]
    except KeyError:
        site = institution

    return site

def simple_get_hosts_ipv4_addrs (hostname):

    if is_a_valid_ip (hostname):
        return [hostname]

    try:
        ip = list (get_dns_addresses (hostname))

    except (socket.error, socket.herror):
        ip = ['0.0.0.0']

    return ip

def get_host_ipv4_addr (host):

    return simple_get_hosts_ipv4_addrs(host)[0]

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

def parse_timestamp_column (series):

    epoch_ns = pd.to_datetime(series) - datetime(1970,1,1)
    epoch = epoch_ns.astype(int) / int(1e9)

    return epoch

def get_squid_host_alias_map (geo_table):

    SHA_map = geo_table[['Host', 'Alias']][~geo_table.IsDNS].drop_duplicates()
    SHA_map['Host'] = SHA_map['Host'].str.lower()
    SHA_map['Alias'][SHA_map.Host == SHA_map.Alias] = ''

    return SHA_map.set_index('Host')['Alias']

def safe_geo_fun (host_id, geo_fun):

    try:
        isp_uc = geo_fun(host_id).encode('utf-8', 'ignore')
        # Spaces are removed in the geolist
        isp = isp_uc.replace(' ', '')

    except (socket.gaierror, AttributeError):
        isp = 'Unknown'

    return isp

class GeoIPWrapper(object):

    def __init__ (self, geoip_db_file):

        self.geoip = pygeoip.GeoIP(geoip_db_file,
                                   flags=pygeoip.MEMORY_CACHE)

        self.org_by_addr = partial(safe_geo_fun,
                                   geo_fun=self.geoip.org_by_addr)

        self.org_by_name = partial(safe_geo_fun,
                                   geo_fun=self.geoip.org_by_name)

class CMSTagger(object):

    def __init__ (self, geo_table, geoip):

        self.geo = geo_table
        self.geoip = geoip

        valid_squids = ~( (self.geo['Ip'] == '0.0.0.0') | self.geo['IsDNS'] )
        Sqd_Ip_Site_df = self.geo[['Site', 'Ip']][valid_squids]
        self.squids_ip_sites_map = self._compact_sites(Sqd_Ip_Site_df)

        Sqd_Insti_Site_df = self.geo[['Institution', 'Site']]
        self.squids_institute_sites_map = self._compact_sites(Sqd_Insti_Site_df)
        self.squids_institute_sites_map['Unknown'] = 'Unknown'

    def tag_hosts (self, data, host_ip_field):

        return tag_hosts( data, host_ip_field, self.squids_institute_sites_map,
                          self.squids_ip_sites_map, self.geo, self.geoip )

    def _compact_sites (self, geo_slice):

        other_field = geo_slice.columns.diff(['Site'])[0]
        slice_cp = geo_slice.drop_duplicates()

        x = slice_cp['Site'].str.split('_', 2)
        slice_cp['BaseSite'] = x.str.get(1) + '_' + x.str.get(2)
        slice_cp['Tier'] =  x.str.get(0)

        z = slice_cp.groupby([other_field, 'BaseSite'])['Tier']
        w = z.agg( lambda df: ",".join(sorted(df.values)) ).reset_index(level='BaseSite')
        w['Sites'] = w['Tier'] + '_' + w['BaseSite']

        compacted = w.groupby(level=other_field)['Sites'].agg( lambda df: "; ".join(sorted(df.values)) )

        return compacted

def cms_site_name_split (site_name):

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

if __name__ == "__main__":

    server_lists = "http://wlcg-squid-monitor.cern.ch/"
    geolist_file = server_lists + "geolist.txt"
    exception_list_file = server_lists + "exceptionlist.txt"

    geo = parse_geolist( get_url( geolist_file))
    actions, WN_view, MO_view = parse_exceptionlist( get_url( exception_list_file))

    print load_awstats_data('cmsfrontier')

