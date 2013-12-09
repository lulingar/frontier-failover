#!/usr/bin/env python

import re
import socket
import pprint

def main():

    geolistfile = '/home/dbfrontier/local/apache/frontier/geolist.txt'

    from_old = parse_geolist_original(geolistfile)
    from_new = parse_geolist_new(geolistfile)

    """
    print from_old
    print "\n *** *** *** \n"
    print from_new
    print "\n *** *** *** \n"
    dd (from_old, from_new, "base")
    """

    pp = pprint.PrettyPrinter (indent=2, width=120)
    str_old = pp.pformat(from_old)
    str_new = pp.pformat(from_new)

    fout = open("/tmp/geo-old.txt", 'w')
    fout.write(str_old)
    fout.close()
    fout = open("/tmp/geo-new.txt", 'w')
    fout.write(str_new)
    fout.close()


def parse_geolist_original (geolistfile, sitesquid={}):

    fobj = open(geolistfile,'r')
    dirlevel = 1
    while 1:
       oneline=fobj.readline()
       if not oneline:
           break
       oneline=oneline.strip()
       if not oneline:
           continue
       elem = oneline.split()[0]
       if elem == '<Directory' or elem=='<Directory>':
          if dirlevel == 1:
             dirlevel+=1
          elif dirlevel == 2:
             key =  oneline.split(' "/')[1].split('">')[0]
             if not sitesquid.has_key(key):
                 sitesquid[key]={}
             dirlevel+=1
       elif elem == '</Directory>':   
          dirlevel-=1
       else:
          if dirlevel==3:
              entry=oneline.split()
              if entry[0]=="LocalSite":
                  site=entry[1].split('"')[1]
                  if not sitesquid[key].has_key(site):
                     sitesquid[key][site]={}
                     sitesquid[key][site]["HttpProxy"]=[]
                  else:
                     pass
              elif entry[0]=="HttpProxy":
                  proxies = entry[1].split('"')[1].replace(';DIRECT','').replace('|',';').split(';')
                  sitesquid[key][site]["HttpProxy"].extend(proxies)
                  allproxies = sitesquid[key][site]["HttpProxy"][:]
                  #sitesquid[key][site]["HttpProxy"]=list(set(allproxies)).sort()
                  sorted_proxies = list(set(allproxies))
                  sorted_proxies.sort()
                  sitesquid[key][site]["HttpProxy"] = sorted_proxies 
              elif entry[0]=="Proxy+":
                  proxies = [ "++++://"+entry[1].split('"')[1]+":++++" ]
                  sitesquid[key][site]["HttpProxy"].extend(proxies)
                  allproxies = sitesquid[key][site]["HttpProxy"][:]
                  #sitesquid[key][site]["HttpProxy"]=list(set(allproxies)).sort()
                  sorted_proxies = list(set(allproxies))
                  sorted_proxies.sort()
                  sitesquid[key][site]["HttpProxy"] = sorted_proxies 
              else:
                  pass
             

    for key in sitesquid.keys(): 
         for site in sitesquid[key].keys():
             sitesquid[key][site]['ProxyHost']=[]
             sitesquid[key][site]['ProxyIP']=[]
             proxies=sitesquid[key][site]['HttpProxy']
             for entry in proxies:
                 urlproxy = entry.split('://')
                 if len(urlproxy)>1:
                     hostname=urlproxy[1].split(':')[0]
                     sitesquid[key][site]['ProxyHost'].append(hostname)

                     try:
                         sitesquid[key][site]['ProxyIP'].extend(socket.gethostbyname_ex(hostname)[2])
                     except:
                         sitesquid[key][site]['ProxyIP'].extend(['0.0.0.0'])
    fobj.close()

    return sitesquid


def simple_get_host_ipv4 (hostname):

    try:
        ip = socket.gethostbyname_ex(hostname)[2]

    except (socket.error, socket.herror):
        ip = ['0.0.0.0']

    return ip

def parse_geolist_new (geolistfile, sites_squids_list={}):

    geo_str = open(geolistfile).read()
    lines = geo_str.replace('"','').split()

    step1 = ' '.join(lines).split('Directory')
    step2 = [ line.split() for line in step1 ]
    step3 = filter (lambda e: len(e) == 6, step2)

    for e in step3:
        institute = re.sub('[/<>]', '', e[0])
        site = e[2]
        proxies = sorted (set (e[4].replace(';DIRECT','').replace(';','|').split('|')))

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


def list_to_dict(l):
    return dict(zip(map(str, range(len(l))), l))

def dd (d1, d2, ctx="", level=0, indentation=4):

    leading = ' ' * (level * indentation)

    print leading, "Changes in " + ctx
    for k in d1:
        if k not in d2:
            print leading, k + " removed from d2"

    for k in d2:

        if k not in d1:
            print leading, k + " added in d2"
            continue

        if d2[k] != d1[k]:
            if type(d2[k]) not in (dict, list):
                print leading, k + " changed in d2 to " + str(d2[k])
            else:

                if type(d1[k]) != type(d2[k]):
                    print leading, k + " changed to " + str(d2[k])
                    continue

                else:
                    if type(d2[k]) == dict:
                        dd(d1[k], d2[k], k, level+1)
                        continue

                    elif type(d2[k]) == list:
                        dd (list_to_dict(d1[k]), list_to_dict(d2[k]), k, level+1)

    print leading, "Done with changes in " + ctx
    return

if __name__ == "__main__":
    main()

