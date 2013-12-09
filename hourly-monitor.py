#!/usr/bin/env python

import copy
import json
import os
import re
import sys
import time
import urllib2
from datetime import datetime, timedelta
from socket import gethostbyname, gethostbyname_ex

# created by Weizhen.Wang@cern.ch, v3, Aug 05, 2010
# Modified by Luis.Linares@cern.ch, 2013

# threshold for a node
threshold_node = 0
# threshold for a site
threshold_site = 10000
# threshold for a node with unkown site
thunknown = 10000

# maximium list length to show
maxlist = 25

geolistfile = '/home/dbfrontier/local/apache/frontier/geolist.txt'
exceptionlistfile = '/home/dbfrontier/local/apache/frontier/exceptionlist.txt'

def main():

    set_environment ()

    print "<html>\n"
    sites_squids_list = parse_geolist (geolistfile)
    parse_exception_list (exceptionlistfile, sites_squids_list)

    print "<p>Frontier hourly access monitor starting at : %s</p>" % datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    print "Threshold for the total non-proxy access from a site is %d queries/hour" % threshold_site

    instances = ["cmsfrontier1", "cmsfrontier2", "cmsfrontier3"]
    url_model = "http://frontier.cern.ch/awstats/awstats.pl?config=%(instance)s&databasebreak=day&day=%(day)d&framename=mainright&output=allhosts"
    joint_access_stats = []

    for instance in instances:
        the_day_1h_ago = ( datetime.today() - timedelta(hours=1) ).day
        url = url_model % {'instance':instance, 'day':the_day_1h_ago}

        instance_access_stats = get_awstats (url)
        joint_access_stats.append (instance_access_stats)

    stats_all_servers = join_stats_from_servers (joint_access_stats)
    hourlyinfo = calculate_hourly_access (stats_all_servers)

    above_threshold = get_above_threshold (hourlyinfo, threshold_node)
    exbadhosts = expandbad (above_threshold, sites_squids_list)
    finalbadhostsinfo = pick_worker_nodes (exbadhosts, sites_squids_list)

    sitestat = gensitestat (hourlyinfo, finalbadhostsinfo, sites_squids_list)
    reportstr = genReport (hourlyinfo, finalbadhostsinfo, sitestat)

    print reportstr
    print "<p>Frontier hourly access monitor done at : %s</p>" % datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    #time.strftime("%a, %d %b %Y %H:%M:%S GMT",time.gmtime(time.time()))
    print "</html>\n"

    store (stats_all_servers)
    return 0 if "No alarms generated" in reportstr else 1

def simple_get_host_ipv4 (hostname):

    try:
        ip = gethostbyname_ex(hostname)[2]
    except:
        ip = ['0.0.0.0']

    return ip


def parse_geolist (geolistfile, sites_squids_list={}):

    geo_str = open(geolistfile).read()
    lines = geo_str.replace('"','').split()

    step1 = ' '.join(lines).split('Directory')
    step2 = [ line.split() for line in step1 ]
    step3 = filter (lambda e: len(e) == 6, step2)

    for e in step3:
        institute = re.sub('[/<>]', '', e[0])
        site = e[2]
        proxies = sorted (set (e[4].replace(';DIRECT','').replace(';','|').split('|')))

        hostnames = [ url.split(':')[1].replace('/','') for url in proxies ]
        host_ips = [ simple_get_host_ipv4(host) for host in hostnames ]

        site_dict = {site: {'HttpProxy': proxies,
                            'ProxyHost': hostnames,
                            'ProxyIP': sum (host_ips, [])}}
        if institute not in sites_squids_list:
            sites_squids_list[institute] = site_dict
        else:
            sites_squids_list[institute].update(site_dict)

    return sites_squids_list


def set_environment ():

    local_home = os.path.expanduser("~/bin")
    os.putenv ("PATH", os.getenv("PATH") + ':' + local_home)


def get_isp (host):

        try:
            ipaddr = gethostbyname (host)
        except:
            ipaddr = 'Unknown'
            isp = 'Unknown'
        else:
            ispdatapipe = os.popen ("geoIP " + ipaddr, "r")
            ispdata = ispdatapipe.read()
            ispdataline = ispdata.split('\n')

            isp = 'Unknown'
            for line in ispdataline:
                a = line.split('=')
                if len(a) == 2 and a[0] == 'ISP':
                    isp = a[1]

        return isp


def parse_exception_list (excpfile, sites_squids_list={}):

    for line in open(excpfile, 'r'):
        oneline = line.strip()
        if not oneline: continue
        entry = oneline.split()

        site = re.sub('[+-]', '', entry[0])

        if site == 'T0_CH_CERN':
            isplist = ['CERNRoutedBackbone']

        else:
            isplist = []
            isp = ''
            for host in entry[1:]:
                if host[0] == '+':
                    isp = get_isp (host[1:])

                if isp == 'Unknown':
                    isp = 'badispname'
                    continue
                else:
                    isplist.append(isp)

            isplist = list(set(isplist))
            if not isplist: continue

        for isp in set(isplist):

            if isp not in sites_squids_list:
                sites_squids_list[isp] = {}

            if site not in sites_squids_list[isp]:
                sites_squids_list[isp][site] = {}
                sites_squids_list[isp][site]["HttpProxy"] = []

            for host in entry[1:]:
                if host[0] == '+':
                    sites_squids_list[isp][site]["HttpProxy"].append ('++++://' + host[1:] + ':++++')
                    all_proxies = sites_squids_list[isp][site]["HttpProxy"][:]
                    sites_squids_list[isp][site]["HttpProxy"] = list (set (all_proxies))

    for isp in sites_squids_list.keys():
        for site in sites_squids_list[isp].keys():
            proxies = sites_squids_list[isp][site]['HttpProxy']
            hostnames = [ url.split(':')[1].replace('/','') for url in proxies ]
            host_ips = [ simple_get_host_ipv4(host) for host in hostnames ]

            sites_squids_list[isp][site].update({'ProxyHost': hostnames,
                                                 'ProxyIP': sum (host_ips, [])})

    return sites_squids_list


def get_awstats (source_url):

    req = urllib2.Request (url = source_url)
    data = urllib2.urlopen(req).read()

    # info is the access today till now
    usefuldata = data.split('Last visit</th></tr>')[1]             \
                     .split('</table></td></tr></table><br />')[0] \
                     .replace('<tr><td class="aws">', '')          \
                     .split('</td></tr>')
    info = {}
    bytedict = {'GB':2**30, 'MB':2**20, 'KB':2**10, 'Bytes':1, 'Byte':1}

    for line in usefuldata:
        aline = line.strip()
        pieces = aline.split('</td><td>')

        if len(pieces) >= 5 :
            host, access, hits, strsize, date = pieces
            access = int(access)
            hits = int(hits)

            tplsize = strsize.split()
            if len(tplsize) > 1 :
                size = float(tplsize[0]) * bytedict[tplsize[1]]
            else :
                size = float(tplsize[0])

            info[host] = {'hostname':host, 'access':access,
                          'size':size, 'date':date}

    return info

def join_stats_from_servers (joint_access_stats):
    # Combine info from all servers
    info = {}
    keyset = set()

    for access_stat in joint_access_stats:
        keyset |= set(access_stat)

    for key in keyset:
        info[key] = {'hostname':key, 'access':0, 'size':0,
                     'date':'01 Jan 2000 - 00:00'}

        for access_stat in joint_access_stats:
            if key in access_stat:
                info[key]['access'] += access_stat[key]['access']
                info[key]['size'] += access_stat[key]['size']
                try:
                    t1 = time.mktime(time.strptime(access_stat[key]['date'],"%d %b %Y - %H:%M"))
                except:
                    t1 = time.time()-3600

                try:
                    t0 = time.mktime(time.strptime(info[key]['date'],"%d %b %Y - %H:%M"))
                except:
                    t0 = time.time()-3600

                if  t1 > t0:
                    info[key]['date'] = access_stat[key]['date']

    return info

def store (info):
    fobj = open ("lasthourfrontier.data", "w")
    json.dump (info, fobj)
    fobj.close ()

def calculate_hourly_access (info):
    """
     Calculate access this hour.
     the difference of "today's visit till now" and "till lasthour"
    """

    hourlyinfo = {}

    data_file = os.path.expanduser("~/apps/Monitor/Apps/lasthourfrontier.data")

    if not os.path.exists(data_file):
        oldinfo = {}
        print "<p>Running first time?</p>"
    else:
        oldinfo = json.load (open (data_file))

    for host in info.iterkeys():
        if host in oldinfo and time.localtime(time.time()-3600)[3] != 0:
            naccess = info[host]['access'] - oldinfo[host]['access']
            nbytes = info[host]['size'] - oldinfo[host]['size']
        else:
            naccess = info[host]['access']
            nbytes = info[host]['size']

        hourlyinfo[host] = dict(info[host]) # copy dict
        hourlyinfo[host]['hourlyaccess'] = naccess
        hourlyinfo[host]['hourlysize'] = nbytes

    return hourlyinfo

def get_above_threshold (hourlyinfo, threshold):
    """
    Compares accesses with threshold
    Result : above_threshold is a dict with bad hosts
    """
    above_threshold = {}

    for host in hourlyinfo.iterkeys():

        if hourlyinfo[host]['hourlyaccess'] > threshold:
            above_threshold[host] = copy.deepcopy (hourlyinfo[host])

    return above_threshold


def expandbad (host_list, sites_squids_list):
    """ Adds info to a host list
         hosts_info is a dict with host as key, host_info as value
         host_info has keys: 'access', 'size', 'ip_addr', 'institution'
    """

    hosts_info = {}

    for host in host_list.iterkeys():

        hosts_info[host] = host_list[host]
        try:
            ipaddr = gethostbyname(host)
        except:
            ipaddr = 'Unknown'
            institution = 'Unknown'

        if ipaddr != 'Unknown':
            institution = get_isp(host)

        hosts_info[host]['ip_addr'] = ipaddr
        hosts_info[host]['institution'] = institution

        ipaddr_list = ipaddr.split('.')
        bsite = []

        if institution in sites_squids_list:
            for site in sites_squids_list[institution].iterkeys():
                for squidip in sites_squids_list[institution][site]["ProxyIP"]:
                    squidip_list = squidip.split('.')

                    if all(x == y for x, y in zip(ipaddr_list[0:2], squidip_list[0:2])):
                        # in2p3 / GRIF
                        if ipaddr_list[0]=='134' and ipaddr_list[1]=='158':
                            if ipaddr_list[2] == squidip_list[2]:
                                if site == 'T2_FR_GRIF':
                                    bsite = [site]
                                else:
                                    if all(e not in bsite for e in ['T2_FR_GRIF', site]):
                                        bsite.append(site)
                            else:
                                if ipaddr_list[2] in ['128','129','130','131','72','88','89','90','91','152','153','154','155','156','157','158','159']:
                                    bsite = ['T2_FR_GRIF']
                                elif not 'T2_FR_GRIF' in bsite:
                                    if not 'T1_FR_CCIN2P3' in bsite:
                                        bsite.append('T1_FR_CCIN2P3')
                                    if not 'T2_FR_CCIN2P3' in bsite:
                                        bsite.append('T2_FR_CCIN2P3')
                        # Elsewhere...
                        else:
                            if not site in bsite:
                                bsite.append(site)

            if not bsite:
                for site in sites_squids_list[institution].keys():
                    bsite.append(site + "*")

        else:
            bsite = ["Unknown"]

        bsite.sort()
        hosts_info[host]['SITE'] = ';'.join(bsite) + ';'

    return hosts_info


def is_worker_node (host_info, sites_squids_list={}):

    institution = host_info['institution']
    ip_address = host_info['ip_addr']
    hourly_traffic = host_info['hourlyaccess']

    if ( ip_address == 'Unknown' or
          institution == 'Unknown' or
           institution not in sites_squids_list ):
        if hourly_traffic > thunknown: return True

    else:
        proxyiplist = [ site['ProxyIP'] for site in sites_squids_list[institution].itervalues() ]
        return (ip_address not in proxyiplist)


def pick_worker_nodes (hosts_info={}, sites_squids_list={}):

    worker_node_func = lambda host, host_info: is_worker_node(host_info, sites_squids_list)
    worker_nodes = filter_dict (worker_node_func, hosts_info)
    return worker_nodes


def filter_dict (test_function, dictionary, deepcopy=True):
    """ Returns a dictionary from a given one for which its (key, value) pairs
        evaluate to True when applied the test_function.
        @param test_function: Function that accepts key, value pairs from the dictionary
                              as its two arguments, and returns True for those that are
                              to be included in the filtered dictionary.
        @param dictionary: Dictionary to be filtered
        @param deepcopy: When True, all values are copied to (instead of referenced in)
                         the returned dictionary. This enables subsequent modification
                         of the returned dictionary without changing the original one.
    """
    key_function = lambda e: test_function(e[0], e[1])
    filtered = dict (filter (key_function, dictionary.iteritems()))
    if deepcopy:
        return copy.deepcopy (filtered)
    else:
        return filtered


def gensitestat (hourlyinfo, finalbadhostsinfo={}, sites_squids_list={}):
# generate site state
# sitestat is a dict. with sitename as key value is again a dict, keys include "hourlyaccess", ...
    sitestat = {}

    for host in finalbadhostsinfo.iterkeys():
        access = finalbadhostsinfo[host]['hourlyaccess']
        ipaddr = finalbadhostsinfo[host]['ip_addr']
        institution = finalbadhostsinfo[host]['institution']
        lastdate = finalbadhostsinfo[host]['date']
        bsite = finalbadhostsinfo[host]['SITE'].replace('*;',';') # site with * means not sure

        bsite += 'end'
        bsite = bsite.replace(';end','')
        if institution in sites_squids_list:
            proxynow = []
            for absite in bsite.split(";"):
                proxynow.extend(sites_squids_list[institution][absite]["HttpProxy"][:])
            proxylist = list(set(proxynow))
        else:
            proxylist = []

        if bsite not in sitestat:
            sitestat[bsite] = {}
            sitestat[bsite]["hourlyaccess"] = access
            sitestat[bsite]["failhosts"] = [host]
            sitestat[bsite]["failinfo"] = [finalbadhostsinfo[host]]
            sitestat[bsite]["proxies"] = proxylist
        else:
            sitestat[bsite]["hourlyaccess"] += access
            sitestat[bsite]["failhosts"].append(host)
            sitestat[bsite]["failinfo"].append(finalbadhostsinfo[host])
            sitestat[bsite]["proxies"] = list(set(proxylist).union(set(sitestat[bsite]["proxies"])))

    for asite in sitestat.iterkeys():
# to be replaced by codes with uniq operations, modified by Ran Du(ran.du@cern.ch), 24-Aug-2012
#               tmp_src=sitestat[asite]["proxies"][:]
#               tmp_src.sort()
#               tmp_proxylist=[]
#               tmp_hostlist=[]
#               tmp_accesslist=[]
#               for aproxy in tmp_src:
#                   if 'http' in aproxy:
#                      tmp_proxylist.append(aproxy)
#                      tmp_host=aproxy.replace('http://','').split(':')[0]
#                      tmp_hostlist.append(tmp_host)
#               for aproxy in tmp_src:
#                   if not 'http' in aproxy:
#                      tmp_host=aproxy.split('://')[1].split(':')[0]
#                      if not tmp_host in tmp_hostlist:
#                        tmp_hostlist.append(tmp_host)
#                        tmp_proxylist.append(aproxy)

# modification: add uniq operations to make the items in sitestat[site]['proxies'] uniq
        tmp_src = sitestat[asite]["proxies"][:]
        tmp_src.sort()
        tmp_proxylist = []
        tmp_hostlist = []
        tmp_accesslist = []
        for aproxy in tmp_src:
            if 'http' in aproxy:
                            #  modification: uniq proxy/host
                tmp_host=aproxy.replace('http://','').split(':')[0]
                try:
                    hostname = gethostbyname_ex(tmp_host)[0]
                except:
                    hostname = tmp_host
                if not hostname in tmp_hostlist:
                    tmp_hostlist.append(hostname)
                    tmp_proxylist.append(aproxy)
        for aproxy in tmp_src:
            if 'http' not in aproxy:
                #  modification: uniq proxy/host
                tmp_host=aproxy.split('://')[1].split(':')[0]
                try:
                    hostname = gethostbyname_ex(tmp_host)[0]
                except:
                    hostname = tmp_host
                if not hostname in tmp_hostlist:
                    tmp_hostlist.append(hostname)
                    tmp_proxylist.append(aproxy)

# add squid access

        for asquidhost in tmp_hostlist:
            if asquidhost in hourlyinfo:
                tmp_accesslist.append(hourlyinfo[asquidhost]['hourlyaccess'])
            else:
                try:
                    resolvedhost = gethostbyname_ex(asquidhost)[0]
                except:
                    resolvedhost = asquidhost
                if hourlyinfo.has_key(resolvedhost):
                    tmp_accesslist.append(hourlyinfo[resolvedhost]['hourlyaccess'])
                else:
                    tmp_accesslist.append(0)

        sitestat[asite]["proxies"] = tmp_hostlist[:]
        sitestat[asite]["squidaccess"] = tmp_accesslist[:]
        sitestat[asite]["hourlysquidaccess"] = sum(sitestat[asite]["squidaccess"])

        sitestat[asite]["failinfo"].sort (key = lambda e: e['hourlyaccess'],
                                          reverse = True)

    return sitestat

def genReport (hourlyinfo, finalbadhostsinfo, sitestat):

    reportstr = ''
    reportstr += "<p>total find %d host in %d</p>\n" % (len(finalbadhostsinfo.keys()),len(hourlyinfo.keys()))
    if not finalbadhostsinfo:
        reportstr += "<p>No alarms generated</p>\n"

    for site in sorted(sitestat.iterkeys()):
        accesssumshown = 0

        if (not site=="Unknown") and (sitestat[site]["hourlyaccess"] > threshold_site):
            reportstr+="<h2>%s</h2>" % site
            reportstr+="<p>Total squid proxy queries this hour: %s</p>" % sitestat[site]["hourlysquidaccess"]
            reportstr+="<p>Total direct queries this hour: %s</p>" % sitestat[site]["hourlyaccess"]
            reportstr+="<h3>Squid proxy accesses:</h3>\n"
            reportstr+='<table  border="1" cellpadding="2" cellspacing="0" width="60%">\n'

            reportstr+="<tr><td>%s</td><td>%s</td></tr>\n" \
                              % ('squid name','queries')

            for id_proxy in range(len(sitestat[site]["proxies"])):
                reportstr+="<tr><td>%s</td><td>%d</td></p>" % (sitestat[site]["proxies"][id_proxy],sitestat[site]["squidaccess"][id_proxy])


            reportstr+="</table>\n"
            reportstr+="<h3>Direct accesses:</h3>\n"
            reportstr+='<table  border="1" cellpadding="2" cellspacing="0" width="100%">\n'
            reportstr+="<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>\n" \
                              % ('host name','IP address','queries','last visit')
            for id_host in range(min(maxlist,len(sitestat[site]["failinfo"]))):
                ahostinfo=sitestat[site]["failinfo"][id_host]
                host=ahostinfo['hostname']
                access=finalbadhostsinfo[host]['hourlyaccess']
                ipaddr=finalbadhostsinfo[host]['ip_addr']
                lastdate=finalbadhostsinfo[host]['date']
                reportstr+="<tr><td>%s</td><td>%s</td><td>%d</td><td>%s</td></tr>\n" \
                              % (host,ipaddr,access,lastdate)
                accesssumshown+=access
            if maxlist<len(sitestat[site]["failinfo"]):
                reportstr+="<tr><td>%d more host(s)</td><td>%s</td><td>%d</td><td>%s</td></tr>\n" \
                              % (len(sitestat[site]["failinfo"])-maxlist,'--',sitestat[site]['hourlyaccess']-accesssumshown,'--')

            reportstr+="</table>\n"

    if "Unknown" in sitestat:
        site="Unknown"
        reportstr+="<h2>%s</h2>" % site
        reportstr+="<h3>Direct accesses:</h3>\n"
        reportstr+='<table  border="1" cellpadding="2" cellspacing="0" width="100%">\n'
        reportstr+="<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>\n" \
                          % ('host name','IP address','queries','last visit','ISP')
        for ahostinfo in sitestat[site]["failinfo"]:
            host = ahostinfo['hostname']
            institution = finalbadhostsinfo[host]['institution']
            access = finalbadhostsinfo[host]['hourlyaccess']
            ipaddr = finalbadhostsinfo[host]['ip_addr']
            lastdate = finalbadhostsinfo[host]['date']
            reportstr += "<tr><td>%s</td><td>%s</td><td>%d</td><td>%s</td><td>%s</td></tr>\n" \
                          % (host,ipaddr,access,lastdate,institution)
        reportstr+="</table>\n"

    if (not '<h2>' in reportstr) and (not 'No alarms' in reportstr):
        reportstr+="<p>No alarms generated</p>\n"

    return reportstr

if __name__ == "__main__":
    sys.exit (main())


