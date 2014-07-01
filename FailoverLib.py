#! /usr/bin/env python

import os
import re
import socket
import sys
import urllib2

import pandas as pd
import pygeoip

from datetime import datetime
from functools import partial
from unidecode import unidecode

def load_awstats_data (machine, base_path, date=None):

    if not date:
        date = datetime.today()
    date_string = date.strftime('%m%Y%d')

    aws_file_tpl = "{base}/{instance}/awstats{date}.{instance}.txt"

    aws_file = os.path.expanduser(aws_file_tpl.format(base=base_path,
                                                      date=date_string,
                                                      instance=machine))
    data = get_awstats_hosts_info(aws_file)

    if len(data):
        dataframe = pd.DataFrame(data)
        del dataframe['Pages']               # Because it is identical to 'Hits'
        del dataframe['Last visit']        # Most of the time is not useful info

    else:
        dataframe = pd.DataFrame(None,
                                 columns=('Host', 'Hits', 'Bandwidth'))

    return dataframe

def get_awstats_hosts_info (awstats_file, parse_timestamps=False):

    aws_list = []
    try:
        awsf = open(awstats_file)
    except IOError:
        sys.stderr.write("I/O Exception when trying to open file {0}.\n".format(awstats_file))
        return aws_list

    # Get binary offset of hosts information
    offset_known = False
    for line in awsf:
        if 'POS_VISITOR' in line:
            start_read = int(line.split()[1])
            offset_known = True
            break

    if not offset_known:
        sys.stderr.write("The file {0} is malformed.\n".format(awstats_file))
        return aws_list

    # Jump to and read hosts information
    awsf.seek(start_read, 0)
    awsf.readline()
    for line in awsf:
        if 'END_VISITOR' in line: break

        fields = line.split()

        #Host - Pages - Hits - Bandwidth - Last visit date - [Start date of last visit] - [Last page of last visit]
        host = fields[0]
        pages = int(fields[1])
        hits = int(fields[2])
        bandwidth = int(fields[3])
        if parse_timestamps:
            last_visit_dt = datetime.strptime(fields[4], "%Y%m%d%H%M%S")
        else:
            last_visit_dt = fields[4]

        aws_list.append({'Host': host, 'Pages': pages, 'Hits': hits,
                        'Bandwidth': bandwidth, 'Last visit': last_visit_dt})
    awsf.close()

    return aws_list

def download_aggregated_awstats_data (machines, base_path, date=None):

    data = {}
    for machine in machines:
        data[machine] = load_awstats_data(machine, base_path, date).set_index('Host')

    panel = pd.Panel(data)
    frame = panel.transpose(minor='items', items='minor', major='major')\
                 .to_frame(filter_observations=True)
    aggregated = frame.groupby(level=0).agg( {'Hits': sum,
                                              'Bandwidth': sum})
    aggregated.index.name = 'Host'
    aggregated.reset_index(inplace=True)

    aggregated['Ip'] = aggregated['Host'].apply(get_host_ipv4_addr)
    aggregated['Hits'] = aggregated['Hits'].astype(int)
    aggregated['Bandwidth'] = aggregated['Bandwidth'].astype(int)

    return aggregated

def get_url (url):

    rq = urllib2.Request (url)
    data = urllib2.urlopen(rq).read()

    return data

def parse_geolist (geolist_raw):

    geo_str_unicode = unicode(geolist_raw, pygeoip.ENCODING)
    geo_str_ascii = unidecode(geo_str_unicode)
    geo_str = geo_str_ascii.replace('"','')
    simply_spaced = ' '.join(geo_str.split())

    step1 = simply_spaced.split('Directory')
    step2 = ( line.split() for line in step1 )
    step3 = filter(lambda e: len(e) == 6, step2)

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

def patch_geo_table (geo, MO_view, WN_view, actions, geoip):

    geo_new = geo[~geo.IsDNS].copy()

    # Add hosts (along with sites) specified to be added as monitoring view
    MO_to_add = MO_view[~(MO_view.Host.isin(geo_new.Alias) | MO_view.Host.isin(geo_new.Host))].copy()
    MO_to_add['Institution'] = MO_to_add.Host.apply(geoip.org_by_name)
    MO_to_add = pd.DataFrame(flatten(gen_geo_entries(r['Host'], r['Institution'], r['Site']) for r in MO_to_add.to_dict('records')))
    geo_new = pd.concat([geo_new, MO_to_add], ignore_index=True)

    # Remove sites with specified removal action from monitoring view
    geo_new = geo_new[ ~geo_new.Site.isin(actions[actions == '-'].index) ]

    # Remove worker nodes in sites with no action specified
    for idx, elem in WN_view[ WN_view.Site.isin(geo_new.Site) ].iterrows():
        sel = ((geo_new.Alias == elem.Host) | (geo_new.Host == elem.Host)) & (geo_new.Site == elem.Site)
        geo_new = geo_new[~sel]

    return geo_new

def tag_hosts (dataframe, host_ip_field, squids_institute_sites_map, squids_ip_sites_map, geo, geoip):

    data = dataframe.copy()
    data['Sites'] = ''
    wn_fun = partial(assign_site_workernode, squids_inst_site_map=squids_institute_sites_map,
                                             geoip=geoip )
    #TODO: Implement IP exception for some French machines

    data['Sites'] = data[host_ip_field].apply(wn_fun)

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

def flatten (iterator):
    return sum(iterator, [])

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

    isp_u = None
    try:
        geo_data = geo_fun(host_id)
        isp_u = unicode(geo_data, pygeoip.ENCODING)
    except (socket.gaierror, AttributeError):
        pass
    except TypeError:
        # Depending on pygeoip version, a geo_fun would already return Unicode
        isp_u = geo_data

    if not isinstance(isp_u, basestring):
        isp_u = u'Unknown'

    isp_uc = unidecode(isp_u)     # Sensibly transliterate non-ASCII characters
    isp = isp_uc.replace(' ', '')           # Spaces are removed in the geolist

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
        slice_cp['Tier'] = x.str.get(0)

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
