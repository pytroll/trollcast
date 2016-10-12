#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2012-2016 SMHI

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""Trollcast client. Leeches all it can :)
todo:
- connection between institutes is shutdown after a while (2 hours ?)
- filename are wrong (1 year to old)
- Option for new log file every day? Now log files are quite big after few days.
- resets connection to mirror in case of timeout.

"""
from __future__ import with_statement

import logging
import os.path
import time
import uuid
import warnings
from ConfigParser import ConfigParser, NoOptionError
from datetime import datetime, timedelta
from Queue import Empty, Queue
from threading import Event, Lock, Thread, Timer
from urlparse import urlsplit, urlunparse

import numpy as np
from zmq import (DEALER, IDENTITY, LINGER, POLLIN, PUSH, SUB, SUBSCRIBE,
                 Context, Poller, zmq_version)

from posttroll import context
from posttroll.message import Message, strp_isoformat
from trollsift import compose

logger = logging.getLogger(__name__)

# TODO: what if a scanline never arrives ? do we wait for it forever ?
# TODO: subscriber reset

# FIXME: this should be configurable depending on the type of data.
LINES_PER_SECOND = 6
LINE_SIZE = 11090 * 2
CLIENT_TIMEOUT = timedelta(seconds=45)

REQ_TIMEOUT = 1  # s
ZMQ_REQ_TIMEOUT = REQ_TIMEOUT * 1000
if zmq_version().startswith("2."):
    ZMQ_REQ_TIMEOUT *= 1000
BUFFER_TIME = 2.0


class Subscriber(object):

    def __init__(self, addresses, translate=False):
        self._addresses = addresses
        self._translate = translate
        self.subscribers = []
        self._poller = Poller()
        for addr in self._addresses:
            subscriber = context.socket(SUB)
            subscriber.setsockopt(SUBSCRIBE, "pytroll")
            subscriber.connect(addr)
            self.subscribers.append(subscriber)
            self._poller.register(subscriber)
        self._lock = Lock()
        self._loop = True

    @property
    def sub_addr(self):
        return dict(zip(self.subscribers, self._addresses))

    @property
    def addr_sub(self):
        return dict(zip(self._addresses, self.subscribers))

    def reset(self, addr):
        with self._lock:
            idx = self._addresses.index(addr)
            self._poller.unregister(self.subscribers[idx])
            self.subscribers[idx].setsockopt(LINGER, 0)
            self.subscribers[idx].close()
            self.subscribers[idx] = context.socket(SUB)
            self.subscribers[idx].setsockopt(SUBSCRIBE, "pytroll")
            self.subscribers[idx].connect(addr)
            self._poller.register(self.subscribers[idx], POLLIN)

    def recv(self, timeout=None):
        """Receive a message, timeout in seconds.
        """

        if timeout:
            timeout *= 1000

        while (self._loop):
            with self._lock:
                if not self._loop:
                    break
                subs = dict(self._poller.poll(timeout=timeout))
                if subs:
                    for sub in self.subscribers:
                        if sub in subs and subs[sub] == POLLIN:
                            msg = Message.decode(sub.recv())
                            if self._translate:
                                url = urlsplit(self.sub_addr[sub])
                                host = url[1].split(":")[0]
                                msg.sender = (
                                    msg.sender.split("@")[0] + "@" + host)
                            yield msg
                else:
                    yield None

    def stop(self):
        """Stop the subscriber
        """
        with self._lock:
            self._loop = False
            for sub in self.subscribers:
                self._poller.unregister(sub)
                sub.setsockopt(LINGER, 0)
                sub.close()


def create_subscriber(cfgfile):
    """Create a new subscriber for all the remote hosts in cfgfile.
    """

    cfg = ConfigParser()
    cfg.read(cfgfile)
    addrs = []
    for host in cfg.get("local_reception", "remotehosts").split():
        addrs.append("tcp://" + cfg.get(host, "hostname") + ":" + cfg.get(
            host, "pubport"))
    localhost = cfg.get("local_reception", "localhost")
    addrs.append("tcp://" + cfg.get(localhost, "hostname") + ":" + cfg.get(
        localhost, "pubport"))
    logger.debug("Subscribing to " + str(addrs))
    return Subscriber(addrs, translate=True)


class RTimer(Thread):

    def __init__(self, tries, warning_message, function, *args, **kwargs):
        Thread.__init__(self)
        self.event = Event()
        self.interval = 40
        self.loop = True
        self.tries = tries
        self.attempt = 0
        self.args = args
        self.kwargs = kwargs
        self.alert_func = function
        self.warning = warning_message

    def reset(self):
        self.event.set()
        self.attempt = 0

    def alert(self):
        self.attempt += 1
        if self.attempt >= self.tries:
            logger.warning(self.warning)
            self.alert_func(*self.args, **self.kwargs)
            self.reset()
        else:
            logger.warning(self.warning)

    def run(self):
        while self.loop:
            self.event.wait(self.interval)
            if not self.event.is_set() and self.loop:
                self.alert()
            elif self.event.is_set():
                self.event.clear()

    def stop(self):
        self.loop = False
        self.event.set()


def reset_subscriber(subscriber, addr):
    logger.warning("Resetting connection to " + addr)
    subscriber.reset(addr)


def create_timers(cfgfile, subscriber):
    cfg = ConfigParser()
    cfg.read(cfgfile)
    addrs = []
    timers = {}
    for host in cfg.get("local_reception", "remotehosts").split():
        addrs.append("tcp://" + cfg.get(host, "hostname") + ":" + cfg.get(
            host, "pubport"))
    localhost = cfg.get("local_reception", "localhost")
    addrs.append("tcp://" + cfg.get(localhost, "hostname") + ":" + cfg.get(
        localhost, "pubport"))
    for addr in addrs:
        timers[addr] = RTimer(1,
                              addr + " seems to be down, no hearbeat received",
                              reset_subscriber, subscriber, addr)

        timers[addr].start()
    return timers


def create_requesters(cfgfile, response_q):
    """Create requesters to all the configure remote hosts.
    """
    cfg = ConfigParser()
    cfg.read(cfgfile)
    station = cfg.get("local_reception", "station")
    requesters = {}
    for host in cfg.get("local_reception", "remotehosts").split() + [cfg.get(
            "local_reception", "localhost")]:
        pubport = cfg.get(host, "pubport")
        hostname, req_port, rep_port = (cfg.get(host, "hostname"), cfg.get(
            host, "reqport"), cfg.get(host, "repport"))
        remote_station = host
        requesters[host] = AsyncRequester(hostname, req_port, rep_port, station, remote_station,
                                          response_q, pubport)
    #host = cfg.get("local_reception", "localhost")
    #pubport = cfg.get(host, "pubport")
    #host, port = (cfg.get(host, "hostname"), cfg.get(host, "reqport"))
    #requesters[host] = AsyncRequester(host, port, station, response_q, pubport)
    return requesters


class ResponseCollector(object):
    """Collect responses."""

    def __init__(self, scanlines):
        """Initialize the collector."""
        self.loop = True
        self.response_queue = Queue()
        self.packets = {}
        self.ids = []
        self.scanlines = scanlines

    def collect(self):
        """Collect the data."""
        vcids = {}
        data = []
        with open('wla.ids', 'wb') as fd_:
            while self.loop:
                try:
                    packet_id, response = self.response_queue.get(True, 2)
                except Empty:
                    continue
                platform_name, packet_info = packet_id
                vcid, count = packet_info
                vcids.setdefault(vcid, []).append(count, response.data)
                # test case 16777215 16777216 1 2
                self.ids.append(packet_id)
                # self.packets[packet_id] = response.data
                # logger.debug('yay, we got new data for %s!', str(req_id))
                fd_.write(str(packet_id) + '\n')


class SimpleRequester(object):
    """Base requester class."""

    request_retries = 3

    def __init__(self, host, req_port, rep_port):
        self._socket = None
        self.req_socket = None
        self.rep_socket = None
        self._reqaddress = "tcp://" + host + ":" + str(req_port)
        self._repaddress = "tcp://" + host + ":" + str(rep_port)
        self._poller = Poller()
        self._lock = Lock()
        self.failures = 0
        self.jammed = False
        self.uuid = uuid.uuid1().bytes

        self.connect()

    def connect(self):
        """Connect to the server
        """
        self.req_socket = context.socket(PUSH)
        self.req_socket.connect(self._reqaddress)

        self._socket = context.socket(DEALER)
        self._socket.setsockopt(IDENTITY, self.uuid)
        self._socket.connect(self._repaddress)
        self._poller.register(self._socket, POLLIN)

    def stop(self):
        """Close the connection to the server
        """
        self.req_socket.setsockopt(LINGER, 0)
        self.req_socket.close()

        self._socket.setsockopt(LINGER, 0)
        self._socket.close()
        self._poller.unregister(self._socket)

    def reset_connection(self):
        """Reset the socket
        """
        with self._lock:
            self.stop()
            self.connect()

    def __del__(self, *args, **kwargs):
        self.stop()


class AsyncRequester(SimpleRequester):

    def __init__(self,
                 host,
                 req_port,
                 rep_port,
                 local_station,
                 remote_station,
                 response_q,
                 pubport=None):
        SimpleRequester.__init__(self, host, req_port, rep_port)
        self.loop = True
        self.missing = dict()
        self.order_queue = Queue()
        self.response_q = response_q
        self.local_station = local_station
        self.remote_station = remote_station
        self.nagger = Thread(target=self.check_replies)
        self.nagger.start()
        self.receiver = Thread(target=self.recv_loop)
        self.receiver.start()
        self._last_ping = None
        self.ping_time = timedelta(hours=1)
        self._ping_fun = None

    def stop(self):
        super(AsyncRequester, self).stop()
        self.loop = False
        try:
            self.nagger.join()
        except RuntimeError:
            pass
        self.receiver.join()

    def recv_loop(self, timeout=ZMQ_REQ_TIMEOUT):
        cnt = 0
        while self.loop:
            with self._lock:
                socks = dict(self._poller.poll(timeout))
            if socks.get(self._socket) == POLLIN:
                with self._lock:
                    req_id, reply = self._socket.recv_multipart()
                try:
                    packet_id = self.missing.pop(req_id)
                except KeyError:
                    continue
                else:
                    rep = Message(rawstr=reply)
                    if rep.binary:
                        logger.debug("Got reply for %s: %s ", packet_id,
                                     " ".join(str(rep).split()[:6]))
                    else:
                        logger.debug("Got reply: " + str(rep))
                    self.failures = 0
                    self.jammed = False
                    if rep.type == 'pong':
                        self.ping_time = datetime.utcnow() - self._last_ping
                        logger.debug("ping roundtrip to " + self.remote_station +
                                     ": " + str(self.ping_time))
                        self._ping_fun(self, self.ping_time)
                    else:
                        self.response_q.put((packet_id, rep))

    def check_replies(self):
        while self.loop:
            try:
                uid, request, next_time, others = self.order_queue.get(True, 2)
            except Empty:
                continue
            secs = (next_time - datetime.utcnow()).total_seconds()

            time.sleep(max(0, secs))

            if uid in self.missing:
                logger.error("Timeout from " + str(self._reqaddress))
                self.stop()
                self.failures += 1
                if self.failures < self.request_retries:
                    self.connect()
                    self.send(request, uid)
                else:
                    self.jammed = True
                    logger.warning("Can't get answer from " + str(
                        self._reqaddress))
                    if others:
                        others[0].place_order(request, others[1:])

    def send(self, request, request_id=None, others=None):
        """Fire and forget
        """
        if request_id is None:
            request_id = uuid.uuid4().bytes
        with self._lock:
            logger.debug("Sending %s", str(request))
            self.req_socket.send_multipart([self.uuid, request_id, str(request)
                                            ])
        self.missing[request_id] = request.data.get('platform_name'), request.data.get('uid')
        if others is None:
            others = []
        self.order_queue.put((request_id, request, datetime.utcnow() +
                              timedelta(seconds=REQ_TIMEOUT), others))

    def place_order(self, request, other_requesters=None):
        self.send(request, others=other_requesters)

    def get_line(self, satellite, uid, other_requesters):
        """Get the scanline of *satellite* at *utctime*.
        """
        msg = Message('/oper/polar/direct_readout/' + self.local_station, 'request',
                      {"type": "scanline",
                       "platform_name": satellite,
                       "uid": uid})
        self.place_order(msg, other_requesters)

    def ping(self, ping_fun):
        """Send a ping.
        """
        msg = Message('/oper/polar/direct_readout/' + self.local_station, 'ping',
                      {"station": self.local_station})
        self._last_ping = datetime.utcnow()
        self._ping_fun = ping_fun
        self.place_order(msg)


class Requester(SimpleRequester):
    """Make a request connection, waiting to get scanlines .
    """

    def __init__(self, host, port, station, pubport=None):
        SimpleRequester.__init__(self, host, port)
        self._station = station
        self._pubport = pubport
        self.blocked = False

    def send(self, msg):
        """Send a message.
        """
        warnings.warn("Send method of Requester is deprecated",
                      DeprecationWarning)
        return self._socket.send(str(msg))

    def recv(self, timeout=None):
        """Receive a message. *timeout* in ms.
        """
        warnings.warn("Recv method of Requester is deprecated",
                      DeprecationWarning)
        if self._poller.poll(timeout):
            self.blocked = False
            return Message(rawstr=self._socket.recv())
        else:
            raise IOError("Timeout from " + str(self._reqaddress))

    def send_async(self, payload):
        self._socket.send_multipart([self.uuid, payload])

    def recv_async(self):
        return self._socket.recv_multipart()[0]

    def ping(self):
        """Send a ping.
        """

        msg = Message('/oper/polar/direct_readout/' + self._station, 'ping',
                      {"station": self._station})
        tic = datetime.now()
        response = self.send_and_recv(msg, ZMQ_REQ_TIMEOUT)
        toc = datetime.now()
        if response is None:
            return None, toc - tic
        else:
            return response.data["station"], toc - tic

    def get_line_async(self):
        pass

    def get_line(self, satellite, uid):
        """Get the scanline of *satellite* at *utctime*.
        """
        msg = Message('/oper/polar/direct_readout/' + self._station, 'request',
                      {"type": "scanline",
                       "platform_name": satellite,
                       "uid": uid})

        try:
            resp = self.send_and_recv(msg, ZMQ_REQ_TIMEOUT)
            if resp.type == "missing":
                return None
            else:
                return resp.data
        except AttributeError:
            return None

    def get_slice(self, satellite, start_time, end_time):
        """Get a slice of scanlines.
        """
        msg = Message('/oper/polar/direct_readout/' + self._station, 'request',
                      {"type": 'scanlines',
                       "satellite": satellite,
                       "start_time": start_time.isoformat(),
                       "end_time": end_time.isoformat()})
        self.send(msg)
        return self.recv(ZMQ_REQ_TIMEOUT).data

    def send_lineinfo(self, sat, utctime, elevation, filename, pos):
        """Send information to our own server.
        """

        msg = Message('/oper/polar/direct_readout/' + self._station, 'notice',
                      {"type": 'scanline',
                       "satellite": sat,
                       "utctime": utctime.isoformat(),
                       "elevation": elevation,
                       "filename": filename,
                       "file_position": pos})
        response = self.send_and_recv(msg, ZMQ_REQ_TIMEOUT)
        return response


def create_publisher(cfgfile):
    cfg = ConfigParser()
    cfg.read(cfgfile)
    try:
        publisher = cfg.get("local_reception", "publisher")
    except NoOptionError:
        return None
    if publisher:
        from posttroll.publisher import NoisyPublisher
        publisher = NoisyPublisher(publisher, 0)
        publisher.start()
        return publisher


class HaveBuffer(Thread):
    """Listen to incomming have messages.
    """

    def __init__(self, cfgfile="sattorrent.cfg"):
        Thread.__init__(self)
        self._sub = create_subscriber(cfgfile)
        self._hb = create_timers(cfgfile, self._sub)
        self._publisher = create_publisher(cfgfile)
        cfg = ConfigParser()
        cfg.read(cfgfile)
        try:
            self._out = cfg.get("local_reception", "output_file")
        except NoOptionError:
            self._out = "./{utctime:%Y%m%d%H%M%S}_{platform_name:_<8s}.trollcast.hmf"
        localhost = cfg.get("local_reception", "localhost")
        self._hostname = cfg.get(localhost, "hostname")
        self._station = cfg.get("local_reception", "station")
        self.scanlines = {}
        self._queues = []
        self.requesters = {}
        self._timers = {}
        self._ping_times = {}
        self._min_ping = timedelta(hours=1)

    def add_queue(self, queue):
        """Adds a queue to dispatch have messages to
        """
        self._queues.append(queue)

    def del_queue(self, queue):
        """Deletes a dispatch queue.
        """
        self._queues.remove(queue)

    def send_to_queues(self, sat, uid):
        """Send scanline at *utctime* to queues.
        """
        try:
            self._timers[(sat, uid)].cancel()
            del self._timers[(sat, uid)]
        except KeyError:
            pass
        for queue in self._queues:
            queue.put_nowait((sat, uid, self.scanlines[sat][uid]))

    def update_ping(self, req, pingtime):
        self._ping_times[req] = pingtime
        pings = self._ping_times.values()
        pings.append(timedelta(hours=1))
        self._min_ping = min(pings)

    def run(self):
        counter = {}
        for message in self._sub.recv(1):
            if message is None:
                continue
            if (message.type == "have"):
                packet_info = message.data.copy()
                sat = packet_info['platform_name']
                uid = packet_info['uid']
                if isinstance(uid, list):
                    uid = tuple(uid)
                # This should take care of address translation.
                senderhost = message.sender.split("@")[1]
                packet_info['origin'] = (
                    senderhost + ":" + packet_info["origin"].split(":")[1])
                packet_info.setdefault('quality', 100)
                packet_info['ping_time'] = self._ping_times.get(
                    senderhost, timedelta(hours=1))
                packet_info['counter'] = counter.setdefault(packet_info['origin'], 0)
                counter[packet_info['origin']] += 1
                # TODO: delete counter when pass is done.

                self.scanlines.setdefault(sat, {})

                requester = self.requesters[packet_info['origin'].split(":")[0]]
                if uid not in self.scanlines[sat]:
                    self.scanlines[sat][uid] = [packet_info]
                    # self.scanlines[sat][uid] = [
                    #    (sender, elevation, quality, self._ping_times.get(senderhost, 3600000))]
                    # TODO: This implies that we always wait BUFFER_TIME before
                    # sending to queue. In the case were the "have" messages of
                    # all servers were sent in less time, we should not be
                    # waiting...
                    if len(self.requesters) == 1 or (
                            packet_info['ping_time'] == self._min_ping and
                            not requester.jammed):
                        self.send_to_queues(sat, uid)
                    else:
                        timer = Timer(BUFFER_TIME,
                                      self.send_to_queues,
                                      args=[sat, uid])
                        timer.start()
                        self._timers[(sat, uid)] = timer
                else:
                    # Since append is atomic in CPython, this should work.
                    # However, if it is not, then this is not thread safe.
                    self.scanlines[sat][uid].append(packet_info)
                    if (len(self.scanlines[sat][uid]) == len(self.requesters)) or
                        (packet_info['ping_time'] == self._min_ping and
                         not requester.jammed):
                        self.send_to_queues(sat, uid)
            elif (message.type == "heartbeat"):
                senderhost = message.sender.split("@")[1]
                sender = ("tcp://" + senderhost + ":" +
                          message.data["addr"].split(":")[1])
                logger.debug("receive heartbeat from " + str(sender) + ": " +
                             str(message))
                self._hb[str(sender)].reset()

                for addr, req in self.requesters.items():
                    # can we get the ip adress from the socket somehow ?
                    # because right now the pubaddr and sender are not the same
                    # (name vs ip)
                    if addr == senderhost:
                        req.ping(self.update_ping)
                        break

    def stop(self):
        """Stop buffering.
        """
        self._sub.stop()
        for timer in self._hb.values():
            timer.stop()
        if self._publisher:
            self._publisher.stop()


def compute_line_times(utctime, start_time, end_time):
    """Compute the times of lines if a swath order depending on a reference
    *utctime*.
    """
    offsets = (np.arange(0, 1, 1.0 / LINES_PER_SECOND) + utctime.microsecond /
               1000000.0)
    offsets = offsets.round(3)
    offsets[offsets > 1] -= 1
    offsets -= start_time.microsecond
    offset = timedelta(seconds=min(abs(offsets)))
    time_diff = (end_time - start_time - offset)
    time_diff = time_diff.seconds + time_diff.microseconds / 1000000.0
    nblines = int(np.ceil(time_diff * LINES_PER_SECOND))
    rst = start_time + offset
    linepos = [rst + timedelta(seconds=round(i * 1.0 / LINES_PER_SECOND, 3))
               for i in range(nblines)]
    linepos = set(linepos)
    return linepos


true_names = {"NOAA 19": "NOAA-19", "NOAA 18": "NOAA-18", "NOAA 15": "NOAA-15"}


class OrderClient(HaveBuffer):
    """The client class.
    """

    def __init__(self, cfgfile="sattorrent.cfg"):
        HaveBuffer.__init__(self, cfgfile)
        self.collector = ResponseCollector(self)
        self.collector_thread = Thread(target=self.collector.collect)
        self.collector_thread.start()
        self.requesters = create_requesters(cfgfile,
                                            self.collector.response_queue)
        self.cfgfile = cfgfile
        self.loop = True

    def get_lines(self, satellite, scanline_dict):
        """Retrieve the best (highest elevation) lines of *scanline_dict*.
        """

        for utctime, hosts in scanline_dict.iteritems():
            hostname, elevation = max(hosts, key=(lambda x: x[1]))
            host = hostname.split(":")[0]

            logger.debug("requesting " + " ".join([str(satellite), str(
                utctime), str(host)]))
            data = self.requesters[host].get_line(satellite, utctime)

            yield utctime, data, elevation

    def order_line(self, requesters, sat, uid):
        """Get a single scanline from requesters

        :param requesters:
        :return:
        """
        # for req in requesters:
        msg = Message('/oper/polar/direct_readout/' + self._station, 'request',
                      {"type": "scanline",
                       "platform_name": sat,
                       "uid": uid})

        requesters[0].place_order(msg, requesters[1:], self.scanlines)

    def get_all(self, satellites):
        """Retrieve all the available scanlines from the stream, and save them.
        """
        #sat_last_seen = {}
        sat_lines = {}
        first_time = None
        for sat in satellites:
            sat_lines[sat] = {}
        queue = Queue()
        self.add_queue(queue)

        while self.loop:
            try:
                sat, uid, senders = queue.get(True, 2)
            except Empty:
                continue

            if sat not in satellites:
                continue

            # if sat not in sat_last_seen:
            #    logger.info("Start receiving data for " + sat)

            logger.debug("Picking line " + " ".join([str(uid), str(senders)]))
            # choose the highest quality, lowest ping time, highest elevation.
            sender_quality = sorted(
                senders,
                key=(
                    lambda x: (x.get('quality', 100), -x['ping_time'], x.get('elevation', 90))
                ))

            requesters = (self.requesters[packet_info['origin'].split(":")[0]]
                          for packet_info in reversed(sender_quality)
                          if not self.requesters[packet_info['origin'].split(":")[0]].jammed)

            self.order_line(requesters)

    def send_lineinfo_to_server(self, *args, **kwargs):
        """Send information to our own server.
        """
        cfg = ConfigParser()
        cfg.read(self.cfgfile)
        host = cfg.get("local_reception", "localhost")
        host, port = (cfg.get(host, "hostname"), cfg.get(host, "reqport"))
        del port
        self.requesters[host].send_lineinfo(*args, **kwargs)

    def stop(self):
        HaveBuffer.stop(self)
        self.loop = False
        for req in self.requesters.values():
            req.stop()


class Client(HaveBuffer):
    """The client class.
    """

    def __init__(self, cfgfile="sattorrent.cfg"):
        HaveBuffer.__init__(self, cfgfile)
        #self._requesters = create_requesters(cfgfile)
        self.collector = ResponseCollector()
        self.collector_thread = Thread(target=self.collector.collect)
        self.collector_thread.start()
        self.requesters = create_requesters(cfgfile,
                                            self.collector.response_queue)
        self.cfgfile = cfgfile
        self.loop = True

    def get_line(self, requesters, sat, uid):
        """Get a single scanline from requesters

        :param requesters:
        :return:
        """
        for idx, req in enumerate(requesters):
            if req.jammed:
                continue
            req.get_line(sat, uid, requesters[idx:])
            # 1 try to get the line from req
            # 2 add it to the scanlines

    def get_all(self, satellites):
        """Retrieve all the available scanlines from the stream, and save them.
        """
        sat_last_seen = {}
        sat_lines = {}
        first_time = None
        for sat in satellites:
            sat_lines[sat] = {}
        queue = Queue()
        self.add_queue(queue)
        try:
            while self.loop:
                try:
                    sat, uid, senders = queue.get(True, 2)
                    if sat not in satellites:
                        continue

                    if sat not in sat_last_seen:
                        logger.info("Start receiving data for " + sat)

                    logger.debug("Picking line " + " ".join([str(uid), str(
                        senders)]))
                    # choose the highest quality, lowest ping time, highest elevation.
                    # sender_quality = sorted(senders,
                    #                                  key=(lambda x: (x.get('quality', 100),
                    #                                                  -x['ping_time'],
                    #                                                  x.get('elevation', 90))))
                    sender_quality = sorted(senders,
                                            key=(lambda x: (-x['ping_time'])))
                    requesters = [
                        self.requesters[packet_info['origin'].split(":")[0]]
                        for packet_info in reversed(sender_quality)
                    ]

                    self.get_line(requesters, sat, uid)
                    continue

                    for packet_info in reversed(sender_quality):
                        best_req = self.requesters[packet_info['origin'].split(
                            ":")[0]]
                        if best_req.jammed:
                            continue
                        sat_last_seen[sat] = datetime.utcnow(
                        ), packet_info.get('elevation')
                        logger.debug("requesting " + " ".join([str(sat), str(
                            uid), str(packet_info['origin']), str(
                                packet_info.get('elevation'))]))
                        # TODO: this should be parallelized, and timed.
                        # TODO: Choking ?
                        line = best_req.get_line(sat, uid)
                        if line is None:
                            logger.warning("Could not retrieve line %s",
                                           str(uid))
                        else:
                            sat_lines[sat][uid] = line
                            if first_time is None and packet_info[
                                    'quality'] == 100:
                                #first_time = uid
                                first_time = datetime.utcnow()
                            break

                    if best_req is None:
                        logger.debug(
                            "No working connection, could not retrieve"
                            " line %s", str(uid))
                        continue

                except Empty:
                    pass
                # TODO: What if uid is not a time ? CADU
                for sat, (uid, elevation) in sat_last_seen.items():
                    if (uid + CLIENT_TIMEOUT < datetime.utcnow() or
                        (uid + timedelta(seconds=3) < datetime.utcnow() and
                         elevation < 0.5 and len(sat_lines[sat]) > 100)):
                        # write the lines to file
                        try:
                            first_time = (first_time or
                                          min(sat_lines[sat].keys()))
                            last_time = max(sat_lines[sat].keys())
                            logger.info(
                                sat +
                                " seems to be inactive now, writing file.")
                            fdict = {}
                            fdict["platform_name"] = sat
                            fdict["utctime"] = first_time

                            filename = compose(self._out, fdict)
                            with open(filename, "wb") as fp_:
                                for linetime in sorted(sat_lines[sat].keys()):
                                    fp_.write(sat_lines[sat][linetime])
                            if self._publisher:
                                to_send = {}
                                to_send["platform_name"] = true_names[sat]
                                to_send["format"] = "HRPT"
                                to_send["start_time"] = first_time
                                to_send["end_time"] = last_time
                                to_send["data_processing_level"] = "0"
                                to_send["uid"] = os.path.basename(filename)
                                fullname = os.path.realpath(filename)
                                to_send["uri"] = urlunparse(
                                    ("ssh", self._hostname, fullname, "", "",
                                     ""))
                                if sat == "NOAA 15":
                                    to_send["sensor"] = ("avhrr/3", "amsu-a",
                                                         "amsu-b", "hirs/3")
                                elif sat in ["NOAA 19", "NOAA 18"]:
                                    to_send["sensor"] = ("avhrr/3", "mhs",
                                                         "amsu-a", "hirs/4")

                                to_send["type"] = "binary"
                                msg = Message(
                                    "/".join(("", to_send["format"], to_send[
                                        "data_processing_level"], self._station
                                    )), "file", to_send)
                                logger.debug("publishing %s", str(msg))
                                self._publisher.send(str(msg))

                        except ValueError:
                            logger.info("Got no lines for " + sat)
                            continue
                        finally:
                            sat_lines[sat] = {}
                            del sat_last_seen[sat]
                            first_time = None
        except KeyboardInterrupt:
            for sat, (uid, elevation) in sat_last_seen.items():
                logger.info(sat + ": writing file.")
                first_time = (first_time or min(sat_lines[sat].keys()))
                filename = first_time.isoformat() + sat + ".hmf"
                with open(filename, "wb") as fp_:
                    for linetime in sorted(sat_lines[sat].keys()):
                        fp_.write(sat_lines[sat][linetime])

                sat_lines[sat] = {}
                del sat_last_seen[sat]
            raise

    def send_lineinfo_to_server(self, *args, **kwargs):
        """Send information to our own server.
        """
        cfg = ConfigParser()
        cfg.read(self.cfgfile)
        host = cfg.get("local_reception", "localhost")
        host, port = (cfg.get(host, "hostname"), cfg.get(host, "reqport"))
        del port
        self.requesters[host].send_lineinfo(*args, **kwargs)

    def stop(self):
        HaveBuffer.stop(self)
        self.loop = False
        for req in self.requesters.values():
            req.stop()
        context.term()
