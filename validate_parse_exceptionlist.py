#!/usr/bin/env python

import json
import os
import re
import pprint
import socket

#import pdb; pdb.set_trace()

debugging = False

def main():

    pp = pprint.PrettyPrinter (indent=2)

    exception_file = "/home/dbfrontier/local/apache/frontier/exceptionlist.txt"
    geolist_file = '/home/dbfrontier/local/apache/frontier/geolist.txt'
    os.putenv("PATH", os.getenv("PATH") + ':' + os.path.expanduser("~/bin"))

    #from_old = parse_geolist (geolist_file, sites_squids_list={}, new=False)
    from_new = parse_geolist (geolist_file, sites_squids_list={}, new=True)
    #if debugging: print "same things?", from_new is from_old

    str_old = pp.pformat(from_new)
    parse_exception_list_new (exception_file, from_new)
    #parse_exception_list_original (exception_file, from_old)

    str_new = pp.pformat(from_new)

    fout = open("/tmp/exc-geo.txt", 'w')
    fout.write(str_old)
    fout.close()
    fout = open("/tmp/exc-new.txt", 'w')
    fout.write(str_new)
    fout.close()


def parse_exception_list_original (excpfile, sitesquid={}):

    geoadd = {}
    mrtgadd = {}
    for line in open(excpfile, 'r'):
        oneline = line.strip()
        if not oneline or oneline.startswith('#'):
            continue

        entry = oneline.split()
        site = entry[0].replace('+','').replace('-','')
        isp = ''
        isplist = []
        for host in entry[1:]:
            if host[0] == '+':
                isp = get_isp(host[1:])

                if isp == 'Unknown':
                    isp = 'badispname'
                    continue
                else:
                    isplist.append(isp)

        isplist = list(set(isplist))

        if not isplist:
            continue

        for isp in isplist:
            if isp in sitesquid:
                if site in sitesquid[isp]:
                    for host in entry[1:]:
                        if host[0] == '+':
                            sitesquid[isp][site]["HttpProxy"].append('++++://'+host[1:]+':++++')
                            allproxies = sitesquid[isp][site]["HttpProxy"][:]
                            sitesquid[isp][site]["HttpProxy"] = list(set(allproxies))
                else:
                    sitesquid[isp][site] = {}
                    sitesquid[isp][site]["HttpProxy"] = []
                    for host in entry[1:]:
                        if host[0] == '+':
                            sitesquid[isp][site]["HttpProxy"].append('++++://'+host[1:]+':++++')
                            allproxies = sitesquid[isp][site]["HttpProxy"][:]
                            sitesquid[isp][site]["HttpProxy"] = list(set(allproxies))

            else:
                sitesquid[isp] = {}
                sitesquid[isp][site] = {}
                sitesquid[isp][site]["HttpProxy"] = []
                for host in entry[1:]:
                    if host[0] == '+':
                        sitesquid[isp][site]["HttpProxy"].append('++++://'+host[1:]+':++++')
                        allproxies = sitesquid[isp][site]["HttpProxy"][:]
                        sitesquid[isp][site]["HttpProxy"] = list(set(allproxies))

        if site == 'T0_CH_CERN':

            isp = 'CERNRoutedBackbone'
            sitesquid[isp][site] = {}
            sitesquid[isp][site]["HttpProxy"] = []
            for host in entry[1:]:
                if host[0] == '+':
                    sitesquid[isp][site]["HttpProxy"].append('++++://'+host[1:]+':++++')
                    allproxies = sitesquid[isp][site]["HttpProxy"][:]
                    sitesquid[isp][site]["HttpProxy"] = list(set(allproxies))

    for isp in sitesquid.keys():
        for site in sitesquid[isp].keys():
            sitesquid[isp][site]['ProxyHost'] = []
            sitesquid[isp][site]['ProxyIP'] = []
            proxies = sitesquid[isp][site]['HttpProxy']
            for entry in proxies:
                urlproxy = entry.split('://')
                if len(urlproxy) > 1:
                    hostname = urlproxy[1].split(':')[0]
                    sitesquid[isp][site]['ProxyHost'].append(hostname)
                    try:
                        sitesquid[isp][site]['ProxyIP'].extend(socket.gethostbyname_ex(hostname)[2])
                    except (socket.error, socket.herror):
                        sitesquid[isp][site]['ProxyIP'].extend(['0.0.0.0'])

    return sitesquid


def parse_exception_list_new (excpfile, sites_squids_list={}):

    site_to_institution_mapfile = os.path.expanduser ('~/conf/sites_institutions.json')
    site_to_institution = init_site_to_institutions_mapping (site_to_institution_mapfile,
                                                             sites_squids_list)
    unknown_institution = 'NewSites'
    sites_squids_list[unknown_institution] = {}

    for line in open(excpfile, 'r'):
        oneline = line.strip()
        if not oneline or oneline.startswith('#'):
            continue

        entry = oneline.split()

        site = re.sub('[+-]', '', entry[0])
        site_action = entry[0][0] if entry[0][0] in '+-' else ''
        if site_action == '-':
            if site in site_to_institution:
                for institution in site_to_institution[site]:
                    sites_squids_list[institution].pop(site, None)
            continue
        elif site_action == '+':
            if site in site_to_institution:
                institutions = site_to_institution[site]
                for institution in institutions:
                    if institution not in sites_squids_list:
                        sites_squids_list[institution] = {site: {}}
                    else:
                        if site not in sites_squids_list[institution]:
                            sites_squids_list[institution][site] = {}
            else:
                institutions = [unknown_institution]
                site_to_institution[site] = institutions
                if site not in sites_squids_list[unknown_institution]:
                    sites_squids_list[unknown_institution][site] = {}
        else:
            try:
                institutions = site_to_institution[site]
            except KeyError:
                print 'The site %s is not registered to an institution. Adding under "%s"...' % (site, unknown_institution)
                institutions = [unknown_institution]
                site_to_institution[site] = institutions
                sites_squids_list[unknown_institution].update({site: {}})

        hosts = entry[1:]
        hosts_to_remove_marked = filter (lambda h: h[0] == '-', hosts)
        hosts_to_remove = set ([ h[1:] for h in hosts_to_remove_marked ])
        hosts_to_add_marked = filter (lambda h: h[0] == '+', hosts)
        hosts_to_add = set ([ h[1:] for h in hosts_to_add_marked ])
        hosts_to_add.discard (hosts_to_remove)

        if len(hosts) != len(hosts_to_add_marked) + len(hosts_to_remove_marked):
            print "Site %s in exception list has a site with no specified action (+,-)" % site

        if debugging:
            print oneline
            print site_action, site, institutions, hosts_to_add, hosts_to_remove

        #if site != 'T0_CH_CERN':

        for institution in institutions:
            proxies = set (sites_squids_list[institution][site].keys())
            proxies.update (hosts_to_add)
            proxies.difference_update (hosts_to_remove)
            if proxies:
                update_institute_hostnames_new (sites_squids_list, institution, site, proxies)
            else:
                sites_squids_list[institution].pop(site)

        """else:
            institution = 'CERNRoutedBackbone'
            if site in sites_squids_list[institution]:
                sites_squids_list[institution].pop(site)"""

    file_obj = open (site_to_institution_mapfile, 'w')
    json.dump (site_to_institution, file_obj, sort_keys=True, indent=4)
    file_obj.close()
    return sites_squids_list

def parse_exception_list_new2 (excpfile, sites_squids_list={}):

    site_to_institution_mapfile = os.path.expanduser ('~/conf/sites_institutions.json')
    site_to_institution = init_site_to_institutions_mapping (site_to_institution_mapfile,
                                                             sites_squids_list)
    unknown_institution = 'NewSites'
    sites_squids_list[unknown_institution] = {}

    for line in open(excpfile, 'r'):
        oneline = line.strip()
        if not oneline or oneline.startswith('#'):
            continue

        entry = oneline.split()

        site = re.sub('[+-]', '', entry[0])
        site_action = entry[0][0] if entry[0][0] in '+-' else ''
        if site_action == '-':
            if site in site_to_institution:
                for institution in site_to_institution[site]:
                    sites_squids_list[institution].pop(site, None)
            continue
        elif site_action == '+':
            if site in site_to_institution:
                institutions = site_to_institution[site]
                for institution in institutions:
                    if institution not in sites_squids_list:
                        sites_squids_list[institution] = {site: {}}
                    else:
                        if site not in sites_squids_list[institution]:
                            sites_squids_list[institution][site] = {}
            else:
                institutions = [unknown_institution]
                site_to_institution[site] = institutions
                if site not in sites_squids_list[unknown_institution]:
                    sites_squids_list[unknown_institution][site] = {}
        else:
            try:
                institutions = site_to_institution[site]
            except KeyError:
                print 'The site %s is not registered to an institution. Adding under "%s"...' % (site, unknown_institution)
                institutions = [unknown_institution]
                site_to_institution[site] = institutions
                sites_squids_list[unknown_institution].update({site: {}})

        hosts = entry[1:]
        hosts_to_remove_marked = filter (lambda h: h[0] == '-', hosts)
        hosts_to_remove = set ([ h[1:] for h in hosts_to_remove_marked ])
        hosts_to_add_marked = filter (lambda h: h[0] == '+', hosts)
        hosts_to_add = set ([ h[1:] for h in hosts_to_add_marked ])
        hosts_to_add.discard (hosts_to_remove)

        if len(hosts) != len(hosts_to_add_marked) + len(hosts_to_remove_marked):
            print "Site %s in exception list has a site with no specified action (+,-)" % site

        if debugging:
            print oneline
            print site_action, site, institutions, hosts_to_add, hosts_to_remove

        #if site != 'T0_CH_CERN':

        for institution in institutions:
            proxies = set (sites_squids_list[institution][site].keys())
            proxies.update (hosts_to_add)
            proxies.difference_update (hosts_to_remove)
            if proxies:
                update_institute_hostnames_new (sites_squids_list, institution, site, proxies)
            else:
                sites_squids_list[institution].pop(site)

        """else:
            institution = 'CERNRoutedBackbone'
            if site in sites_squids_list[institution]:
                sites_squids_list[institution].pop(site)"""

    file_obj = open (site_to_institution_mapfile, 'w')
    json.dump (site_to_institution, file_obj, sort_keys=True, indent=4)
    file_obj.close()
    return sites_squids_list


def init_site_to_institutions_mapping (mapping_file, sites_squids_list):

    try:
        file_obj = open (mapping_file, 'r')
        site_to_institution = json.load (file_obj)
        file_obj.close()
        for institution, sites in sites_squids_list.items():
            for site in sites.keys():
                if institution not in site_to_institution[site]:
                    print "Inconsistency (base, geo) for site %s:" % site, institution, site_to_institution[site]

    except IOError:
        site_to_institution = {}
        for institution, sites in sites_squids_list.items():
            for site in sites.keys():
                if site in site_to_institution:
                    site_to_institution[site].append(institution)
                else:
                    site_to_institution[site] = [institution]

    return site_to_institution

def get_isp (host):

    try:
        ipaddr = socket.gethostbyname (host)

    except (socket.error, socket.herror):
        ipaddr = 'Unknown'

    finally:
        ispdatapipe = os.popen ("geoIP " + ipaddr, "r")
        ispdata = ispdatapipe.read()

        isp = 'Unknown'
        for line in ispdata.splitlines():
            a = line.split('=')
            if len(a) == 2 and a[0] == 'ISP':
                isp = a[1]

    return isp

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
        return len(parts) == 4 and all([(int(x) >= 0) and (int(x) < 256) for x in parts])
    except:
        return False

def parse_geolist (geolistfile, sites_squids_list={}, new=False):

    geo_str = unicode (open(geolistfile).read(), errors='ignore').encode('utf-8')
    lines = geo_str.replace('"','').split()

    step1 = ' '.join(lines).split('Directory')
    step2 = [ line.split() for line in step1 ]
    step3 = filter (lambda e: len(e) == 6, step2)

    for e in step3:
        institute = re.sub('[/<>]', '', e[0])
        site = e[2]
        proxies = sorted (set (e[4].replace(';DIRECT','').replace(';','|').split('|')))

        if new:
            update_institute_hostnames_new (sites_squids_list, institute, site, proxies)
        else:
            update_institute_hostnames (sites_squids_list, institute, site, proxies)

    return sites_squids_list

def update_institute_hostnames (sites_squids_list, institute, site, proxies_list):

    hostnames = [ url.split(':')[1].replace('/','') for url in proxies_list ]
    host_ips = [ simple_get_host_ipv4(host) for host in hostnames ]

    site_dict = {site: {'HttpProxy': proxies_list,
                        'ProxyHost': hostnames,
                        'ProxyIP': sum (host_ips, [])}}

    if institute not in sites_squids_list:
        sites_squids_list[institute] = site_dict
    else:
        sites_squids_list[institute].update(site_dict)

def update_institute_hostnames_new (sites_squids_list, institute, site, proxies_list):

    hosts_dict = {}
    for proxy in proxies_list:
        host_data = proxy.replace('/','').split(':')
        if len(host_data) == 1:
            host_name = host_data[0]
            protocol = port = ''
        else:
            protocol, host_name, port = host_data

        ip_addresses = simple_get_host_ipv4 (host_name)
        hosts_dict[host_name] = {'port': port, 'ip':ip_addresses}

    if institute not in sites_squids_list:
        sites_squids_list[institute] = {site: hosts_dict}
    else:
        sites_squids_list[institute][site] = hosts_dict


def get_dns_addresses (hostname):

    info = socket.getaddrinfo (hostname, 0)
    return set([ e[4][0] for e in info ])

if __name__ == "__main__":
    main()
