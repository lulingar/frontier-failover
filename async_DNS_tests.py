#! /usr/bin/env python

import Queue
import re
import socket
import urllib2
import select

import pycares
import pyuv

def parse_geolist_new (geolistdata):

    geo_str = unicode (geolistdata, errors='ignore').encode('utf-8')
    lines = geo_str.replace('"','').split()

    step1 = ' '.join(lines).split('Directory')
    step2 = [ line.split() for line in step1 ]
    step3 = filter (lambda e: len(e) == 6, step2)

    squids = {}
    hosts = []

    for e in step3:
        institution = re.sub('[/<>]', '', e[0])
        site = e[2]
        proxies = set( e[4].strip(';DIRECT').replace(';','|').split('|') )

        for proxy in proxies:
            host_data = proxy.replace('/','').split(':')
            if len(host_data) == 1:
                host_name = host_data[0]
                protocol = port = ''
            else:
                protocol, host_name, port = host_data

            squids[host_name] = {'institution': institution,
                                 'site': site,
                                 'port': port}
            hosts.append(host_name)

    ip_adds = resolve_hosts(hosts)
    rows = []

    for host_name, ipdata in ip_adds.iteritems():

        names = [host_name,]
        names.extend(ipdata['name'])
        names.extend(ipdata['alias'])

        for host, ip in it.product( names, ipdata['ip']):
            host_data = copy.deepcopy(squids[host_name])
            host_data.update({'host':host, 'ip':ip})
            rows.append(host_data)

    return pd.DataFrame(rows).drop_duplicates()

def thread_launch (input_l, function):

    threads = []
    out_q = Queue.Queue()

    worker = lambda element, queue: queue.put(element, function(element))

    for element in input_l.iteritems():
        threads.append( threading.Thread( target = worker,
                                          args = (element, out_q,)))

    for thread in threads: thread.start()
    for thread in threads: thread.join()

    out_l = {}
    while not out_q.empty():
        element, output = out_q.get()
        out_l[element] = output

    return out_l

def wait_channel(channel):
    while True:
        read_fds, write_fds = channel.getsock()
        if not read_fds and not write_fds:
            break
        timeout = channel.timeout()
        if not timeout:
            channel.process_fd(pycares.ARES_SOCKET_BAD, pycares.ARES_SOCKET_BAD)
            continue
        rlist, wlist, xlist = select.select(read_fds, write_fds, [], timeout)
        for fd in rlist:
            channel.process_fd(fd, pycares.ARES_SOCKET_BAD)
        for fd in wlist:
            channel.process_fd(pycares.ARES_SOCKET_BAD, fd)

class DNSResolver(object):

    def __init__(self, loop):
        self._channel = pycares.Channel(sock_state_cb=self._sock_state_cb)
        self.loop = loop
        self._timer = pyuv.Timer(self.loop)
        self._fd_map = {}

    def _sock_state_cb(self, fd, readable, writable):
        if readable or writable:
            if fd not in self._fd_map:
                # New socket
                handle = pyuv.Poll(self.loop, fd)
                handle.fd = fd
                self._fd_map[fd] = handle
            else:
                handle = self._fd_map[fd]
            if not self._timer.active:
                self._timer.start(self._timer_cb, 1.0, 1.0)
            handle.start(pyuv.UV_READABLE if readable else 0 | pyuv.UV_WRITABLE if writable else 0, self._poll_cb)
        else:
            # Socket is now closed
            handle = self._fd_map.pop(fd)
            handle.close()
            if not self._fd_map:
                self._timer.stop()

    def _timer_cb(self, timer):
        self._channel.process_fd(pycares.ARES_SOCKET_BAD, pycares.ARES_SOCKET_BAD)

    def _poll_cb(self, handle, events, error):
        read_fd = handle.fd
        write_fd = handle.fd
        if error is not None:
            # There was an error, pretend the socket is ready
            self._channel.process_fd(read_fd, write_fd)
            return
        if not events & pyuv.UV_READABLE:
            read_fd = pycares.ARES_SOCKET_BAD
        if not events & pyuv.UV_WRITABLE:
            write_fd = pycares.ARES_SOCKET_BAD
        self._channel.process_fd(read_fd, write_fd)

    def query(self, query_type, name, cb):
        self._channel.query(query_type, name, cb)

    def gethostbyname(self, name, cb):
        self._channel.gethostbyname(name, socket.AF_INET, cb)

def resolve_hosts (hostnames_list):

    def insert_function (queue_, host_, result, error):
        queue_.put( (host_, result) )

    #channel = pycares.Channel()
    loop = pyuv.Loop.default_loop()
    resolver = DNSResolver(loop)

    d_q = Queue.Queue()
    callbacks = []
    for host in hostnames_list:
        callback = fn.partial(insert_function, d_q, host)
        """channel.gethostbyname( host, socket.AF_INET,
                               callback )"""
        resolver.gethostbyname( host, callback)

    """wait_channel(channel)
    channel.destroy()"""
    loop.run()

    out = {}
    while not d_q.empty():

        host, data = d_q.get()
        print host, data
        if data:
            out[host] = {'name': data.name,
                         'alias': data.aliases,
                         'ip': data.addresses}
        else:
            out[host] = {'name': host,
                         'alias': [],
                         'ip': ['unknown',]}

    return out


if __name__ == "__main__":

    import time

    tic = time.time()
    ret = parse_geolist_new( get_url( "http://wlcg-squid-monitor.cern.ch/geolist.txt"))
    time_new = time.time() - tic

    print time_new
    print ret

