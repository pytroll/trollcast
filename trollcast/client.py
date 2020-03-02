#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2012 - 2019 Pytroll

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


import logging
import os.path
import warnings
from configparser import ConfigParser, NoOptionError
from datetime import datetime, timedelta
from queue import Empty, Queue
from threading import Event, Lock, Thread, Timer
from urllib.parse import urlsplit, urlunparse

import numpy as np
from zmq import (LINGER, POLLIN, REQ, SUB, SUBSCRIBE, Context, Poller,
                 zmq_version)


from posttroll import get_context
from posttroll.message import Message, strp_isoformat
from trollsift import compose

logger = logging.getLogger(__name__)

# TODO: what if a scanline never arrives ? do we wait for it forever ?
# TODO: subscriber reset

# FIXME: this should be configurable depending on the type of data.
LINES_PER_SECOND = 6
LINE_SIZE = 11090 * 2
CLIENT_TIMEOUT = timedelta(seconds=45)

REQ_TIMEOUT = 1000
if zmq_version().startswith("2."):
    REQ_TIMEOUT *= 1000
BUFFER_TIME = 2.0


class Subscriber(object):

    def __init__(self, addresses, translate=False):
        self._addresses = addresses
        self._translate = translate
        self.subscribers = []
        self._poller = Poller()
        for addr in self._addresses:

            subscriber = get_context().socket(SUB)
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
            self.subscribers[idx] = get_context().socket(SUB)
            self.subscribers[idx].setsockopt(SUBSCRIBE, "pytroll")
            self.subscribers[idx].connect(addr)
            self._poller.register(self.subscribers[idx], POLLIN)

    def recv(self, timeout=None):
        """Receive a message, timeout in seconds.
        """

        if timeout:
            timeout *= 1000

        while(self._loop):
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
                                msg.sender = (msg.sender.split("@")[0]
                                              + "@" + host)
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
        addrs.append("tcp://" +
                     cfg.get(host, "hostname") + ":" + cfg.get(host, "pubport"))
    localhost = cfg.get("local_reception", "localhost")
    addrs.append("tcp://" +
                 cfg.get(localhost, "hostname") + ":" +
                 cfg.get(localhost, "pubport"))
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
        addrs.append("tcp://" +
                     cfg.get(host, "hostname") + ":" + cfg.get(host, "pubport"))
    localhost = cfg.get("local_reception", "localhost")
    addrs.append("tcp://" +
                 cfg.get(localhost, "hostname") + ":" +
                 cfg.get(localhost, "pubport"))
    for addr in addrs:
        timers[addr] = RTimer(1, addr + " seems to be down, no hearbeat received",
                              reset_subscriber, subscriber, addr)

        timers[addr].start()
    return timers


def create_requesters(cfgfile):
    """Create requesters to all the configure remote hosts.
    """
    cfg = ConfigParser()
    cfg.read(cfgfile)
    station = cfg.get("local_reception", "station")
    requesters = {}
    for host in cfg.get("local_reception", "remotehosts").split():
        pubport = cfg.get(host, "pubport")
        host, port = (cfg.get(host, "hostname"), cfg.get(host, "reqport"))
        requesters[host] = Requester(host, port, station, pubport)
    host = cfg.get("local_reception", "localhost")
    pubport = cfg.get(host, "pubport")
    host, port = (cfg.get(host, "hostname"), cfg.get(host, "reqport"))
    url = "tcp://" + host + ":" + port
    requesters[host] = Requester(host, port, station, pubport)
    return requesters


class SimpleRequester(object):

    """Base requester class.
    """

    request_retries = 3

    def __init__(self, host, port):
        self._socket = None
        self._reqaddress = "tcp://" + host + ":" + str(port)
        self._poller = Poller()
        self._lock = Lock()
        self.failures = 0
        self.jammed = False

        self.connect()

    def connect(self):
        """Connect to the server
        """
        self._socket = get_context().socket(REQ)
        self._socket.connect(self._reqaddress)
        self._poller.register(self._socket, POLLIN)

    def stop(self):
        """Close the connection to the server
        """
        self._socket.setsockopt(LINGER, 0)
        self._socket.close()
        self._poller.unregister(self._socket)

    def reset_connection(self):
        """Reset the socket
        """
        self.stop()
        self.connect()

    def __del__(self, *args, **kwargs):
        self.stop()

    def send_and_recv(self, msg, timeout=REQ_TIMEOUT):

        logger.debug("Locking and requesting: " + str(msg))
        with self._lock:
            retries_left = self.request_retries
            request = str(msg)
            self._socket.send(request)
            rep = None
            while retries_left:
                socks = dict(self._poller.poll(timeout))
                if socks.get(self._socket) == POLLIN:
                    reply = self._socket.recv()
                    if not reply:
                        logger.error("Empty reply!")
                        break
                    rep = Message(rawstr=reply)
                    if rep.binary:
                        logger.debug("Got reply: "
                                     + " ".join(str(rep).split()[:6]))
                    else:
                        logger.debug("Got reply: " + str(rep))
                    self.failures = 0
                    self.jammed = False
                    break
                else:
                    logger.warning("Timeout from " + str(self._reqaddress)
                                   + ", retrying...")
                    # Socket is confused. Close and remove it.
                    self.stop()
                    retries_left -= 1
                    if retries_left <= 0:
                        logger.error("Server doesn't answer, abandoning... " +
                                     str(self._reqaddress))
                        self.connect()
                        self.failures += 1
                        if self.failures == 5:
                            logger.critical("Server jammed ? %s",
                                            self._reqaddress)
                            self.jammed = True
                        break
                    logger.info("Reconnecting and resending " + str(msg))
                    # Create new connection
                    self.connect()
                    self._socket.send(request)
        logger.debug("Release request lock")
        return rep


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

    def ping(self):
        """Send a ping.
        """

        msg = Message('/oper/polar/direct_readout/' + self._station,
                      'ping',
                      {"station": self._station})
        tic = datetime.now()
        response = self.send_and_recv(msg, REQ_TIMEOUT)
        toc = datetime.now()
        if response is None:
            return None, toc - tic
        else:
            return response.data["station"], toc - tic

    def get_line(self, satellite, utctime):
        """Get the scanline of *satellite* at *utctime*.
        """
        msg = Message('/oper/polar/direct_readout/' + self._station,
                      'request',
                      {"type": "scanline",
                       "satellite": satellite,
                       "utctime": utctime.isoformat()})

        try:
            resp = self.send_and_recv(msg, REQ_TIMEOUT)
            if resp.type == "missing":
                return None
            else:
                return resp.data
        except AttributeError:
            return None

    def get_slice(self, satellite, start_time, end_time):
        """Get a slice of scanlines.
        """
        msg = Message('/oper/polar/direct_readout/' + self._station,
                      'request',
                      {"type": 'scanlines',
                       "satellite": satellite,
                       "start_time": start_time.isoformat(),
                       "end_time": end_time.isoformat()})
        self.send(msg)
        return self.recv(REQ_TIMEOUT).data

    def send_lineinfo(self, sat, utctime, elevation, filename, pos):
        """Send information to our own server.
        """

        msg = Message('/oper/polar/direct_readout/' + self._station,
                      'notice',
                      {"type": 'scanline',
                       "satellite": sat,
                       "utctime": utctime.isoformat(),
                       "elevation": elevation,
                       "filename": filename,
                       "file_position": pos})
        response = self.send_and_recv(msg, REQ_TIMEOUT)
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
            self._out = "./{utctime:%Y%m%d%H%M%S}_{platform:4s}_{number:2s}.trollcast.hmf"
        localhost = cfg.get("local_reception", "localhost")
        self._hostname = cfg.get(localhost, "hostname")
        self._station = cfg.get("local_reception", "station")
        self.scanlines = {}
        self._queues = []
        self._requesters = {}
        self._timers = {}
        self._ping_times = {}

    def add_queue(self, queue):
        """Adds a queue to dispatch have messages to
        """
        self._queues.append(queue)

    def del_queue(self, queue):
        """Deletes a dispatch queue.
        """
        self._queues.remove(queue)

    def send_to_queues(self, sat, utctime):
        """Send scanline at *utctime* to queues.
        """
        try:
            self._timers[(sat, utctime)].cancel()
            del self._timers[(sat, utctime)]
        except KeyError:
            pass
        for queue in self._queues:
            queue.put_nowait((sat, utctime, self.scanlines[sat][utctime]))

    def run(self):

        for message in self._sub.recv(1):
            if message is None:
                continue
            if(message.type == "have"):
                sat = message.data["satellite"]
                utctime = strp_isoformat(message.data["timecode"])
                # This should take care of address translation.
                senderhost = message.sender.split("@")[1]
                sender = (senderhost + ":" +
                          message.data["origin"].split(":")[1])
                elevation = message.data["elevation"]
                try:
                    quality = message.data["quality"]
                except KeyError:
                    quality = 100
                self.scanlines.setdefault(sat, {})
                if utctime not in self.scanlines[sat]:
                    self.scanlines[sat][utctime] = [
                        (sender, elevation, quality, self._ping_times.get(senderhost, 3600000))]
                    # TODO: This implies that we always wait BUFFER_TIME before
                    # sending to queue. In the case were the "have" messages of
                    # all servers were sent in less time, we should not be
                    # waiting...
                    if len(self._requesters) == 1:
                        self.send_to_queues(sat, utctime)
                    else:
                        timer = Timer(BUFFER_TIME,
                                      self.send_to_queues,
                                      args=[sat, utctime])
                        timer.start()
                        self._timers[(sat, utctime)] = timer
                else:
                    # Since append is atomic in CPython, this should work.
                    # However, if it is not, then this is not thread safe.
                    self.scanlines[sat][utctime].append((sender, elevation,
                                                         quality, self._ping_times.get(senderhost, 3600000)))
                    if (len(self.scanlines[sat][utctime]) ==
                            len(self._requesters)):
                        self.send_to_queues(sat, utctime)
            elif(message.type == "heartbeat"):
                senderhost = message.sender.split("@")[1]
                sender = ("tcp://" + senderhost +
                          ":" + message.data["addr"].split(":")[1])
                logger.debug("receive heartbeat from " + str(sender) +
                             ": " + str(message))
                self._hb[str(sender)].reset()

                for addr, req in self._requesters.items():
                    # can we get the ip adress from the socket somehow ?
                    # because right now the pubaddr and sender are not the same
                    # (name vs ip)
                    if addr == senderhost:
                        rstation, rtime = req.ping()
                        if rstation is None:
                            logger.warning("Can't ping " + str(sender))
                        else:
                            rtime = (rtime.seconds * 1000 +
                                     rtime.microseconds / 1000.0)
                            self._ping_times[senderhost] = rtime
                            logger.debug("ping roundtrip to " + rstation +
                                         ": " + str(rtime) +
                                         "ms")
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
    offsets = (np.arange(0, 1, 1.0 / LINES_PER_SECOND) +
               utctime.microsecond / 1000000.0)
    offsets = offsets.round(3)
    offsets[offsets > 1] -= 1
    offsets -= start_time.microsecond
    offset = timedelta(seconds=min(abs(offsets)))
    time_diff = (end_time - start_time - offset)
    time_diff = time_diff.seconds + time_diff.microseconds / 1000000.0
    nblines = int(np.ceil(time_diff * LINES_PER_SECOND))
    rst = start_time + offset
    linepos = [rst + timedelta(seconds=round(i * 1.0 /
                                             LINES_PER_SECOND, 3))
               for i in range(nblines)]
    linepos = set(linepos)
    return linepos


true_names = {"NOAA 19": "NOAA-19",
              "NOAA 18": "NOAA-18",
              "NOAA 15": "NOAA-15"}


class Client(HaveBuffer):

    """The client class.
    """

    def __init__(self, cfgfile="sattorrent.cfg"):
        HaveBuffer.__init__(self, cfgfile)
        self._requesters = create_requesters(cfgfile)
        self.cfgfile = cfgfile
        self.loop = True

    def get_lines(self, satellite, scanline_dict):
        """Retrieve the best (highest elevation) lines of *scanline_dict*.
        """

        for utctime, hosts in scanline_dict.items():
            hostname, elevation = max(hosts, key=(lambda x: x[1]))
            host = hostname.split(":")[0]

            logger.debug("requesting " + " ".join([str(satellite),
                                                   str(utctime),
                                                   str(host)]))
            data = self._requesters[host].get_line(satellite, utctime)

            yield utctime, data, elevation

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
                    sat, utctime, senders = queue.get(True, 2)
                    if sat not in satellites:
                        continue

                    if sat not in sat_last_seen:
                        logger.info("Start receiving data for " + sat)

                    logger.debug("Picking line " + " ".join([str(utctime),
                                                             str(senders)]))
                    # choose the highest quality, lowest ping time, highest elevation.
                    sender_elevation_quality = sorted(senders,
                                                      key=(lambda x: (x[2],
                                                                      -x[3],
                                                                      x[1])))
                    best_req = None
                    for sender, elevation, quality, ping_time in reversed(sender_elevation_quality):
                        best_req = self._requesters[sender.split(":")[0]]
                        if best_req.jammed:
                            continue
                        sat_last_seen[sat] = datetime.utcnow(), elevation
                        logger.debug("requesting " +
                                     " ".join([str(sat), str(utctime),
                                               str(sender), str(elevation)]))
                        # TODO: this should be parallelized, and timed. In case of
                        # TODO: Choking ?
                        line = best_req.get_line(sat, utctime)
                        if line is None:
                            logger.warning("Could not retrieve line %s",
                                           str(utctime))
                        else:
                            sat_lines[sat][utctime] = line
                            if first_time is None and quality == 100:
                                first_time = utctime
                            break

                    if best_req is None:
                        logger.debug("No working connection, could not retrieve"
                                     " line %s", str(utctime))
                        continue

                except Empty:
                    pass
                for sat, (utctime, elevation) in sat_last_seen.items():
                    if (utctime + CLIENT_TIMEOUT < datetime.utcnow() or
                        (utctime + timedelta(seconds=3) < datetime.utcnow() and
                         elevation < 0.5 and
                         len(sat_lines[sat]) > 100)):
                        # write the lines to file
                        try:
                            first_time = (first_time
                                          or min(sat_lines[sat].keys()))
                            last_time = max(sat_lines[sat].keys())
                            logger.info(sat +
                                        " seems to be inactive now, writing file.")
                            fdict = {}
                            fdict["platform"], fdict["number"] = sat.split()
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
                                to_send["variant"] = 'DR'
                                to_send["uid"] = os.path.basename(
                                    filename)
                                fullname = os.path.realpath(filename)
                                to_send["uri"] = urlunparse(("ssh",
                                                             self._hostname,
                                                             fullname,
                                                             "",
                                                             "",
                                                             ""))
                                if sat == "NOAA 15":
                                    to_send["sensor"] = ("avhrr/3",
                                                         "amsu-a",
                                                         "amsu-b",
                                                         "hirs/3")
                                elif sat in ["NOAA 19", "NOAA 18"]:
                                    to_send["sensor"] = ("avhrr/3",
                                                         "mhs",
                                                         "amsu-a",
                                                         "hirs/4")

                                to_send["type"] = "binary"
                                msg = Message("/".join(
                                    ("",
                                     to_send["format"],
                                     to_send[
                                         "data_processing_level"],
                                     self._station)),
                                    "file",
                                    to_send)
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
            for sat, (utctime, elevation) in list(sat_last_seen.items()):
                logger.info(sat + ": writing file.")
                first_time = (first_time
                              or min(sat_lines[sat].keys()))
                filename = first_time.isoformat() + sat + ".hmf"
                with open(filename, "wb") as fp_:
                    for linetime in sorted(sat_lines[sat].keys()):
                        fp_.write(sat_lines[sat][linetime])

                sat_lines[sat] = {}
                del sat_last_seen[sat]
            raise

    def order(self, time_slice, satellite, filename):
        """Get all the scanlines for a *satellite* within a *time_slice* and
        save them in *filename*. The scanlines will be saved in a contiguous
        manner.
        """
        start_time = time_slice.start
        end_time = time_slice.stop

        saved = []

        # Create a file of the right length, filled with zeros. The alternative
        # would be to store all the scanlines in memory.
        tsize = (end_time - start_time).seconds * LINES_PER_SECOND * LINE_SIZE
        with open(filename, "wb") as fp_:
            fp_.write("\x00" * (tsize))

        # Do the retrieval.
        with open(filename, "r+b") as fp_:

            queue = Queue()
            self.add_queue(queue)

            linepos = None

            lines_to_get = {}

            # first, get the existing scanlines from self (client)
            logger.info("Getting list of existing scanlines from client.")
            for utctime, hosts in self.scanlines.get(satellite, {}).items():
                if(utctime >= start_time and
                   utctime < end_time and
                   utctime not in saved):
                    lines_to_get[utctime] = hosts

            # then, get scanlines from the server
            logger.info("Getting list of existing scanlines from server.")
            for host, req in self._requesters.items():
                try:
                    response = req.get_slice(satellite, start_time, end_time)
                    for utcstr, elevation in response:
                        utctime = strp_isoformat(utcstr)
                        lines_to_get.setdefault(utctime, []).append((host,
                                                                     elevation))
                except IOError as e__:
                    logger.warning(e__)

            # get lines with highest elevation and add them to current scene
            logger.info("Getting old scanlines.")
            for utctime, data, elevation in self.get_lines(satellite,
                                                           lines_to_get):
                if linepos is None:
                    linepos = compute_line_times(utctime, start_time, end_time)

                time_diff = utctime - start_time
                time_diff = (time_diff.seconds
                             + time_diff.microseconds / 1000000.0)
                pos = LINE_SIZE * int(np.floor(time_diff * LINES_PER_SECOND))
                fp_.seek(pos, 0)
                fp_.write(data)
                self.send_lineinfo_to_server(satellite, utctime, elevation,
                                             filename, pos)
                saved.append(utctime)
                linepos -= set([utctime])

            # then, get the newly arrived scanlines
            logger.info("Getting new scanlines")
            #timethres = datetime.utcnow() + CLIENT_TIMEOUT
            delay = timedelta(days=1000)
            timethres = datetime.utcnow() + delay
            while ((start_time > datetime.utcnow()
                    or timethres > datetime.utcnow())
                   and ((linepos is None) or (len(linepos) > 0))):
                try:
                    sat, utctime, senders = queue.get(True,
                                                      CLIENT_TIMEOUT.seconds)
                    logger.debug("Picking line " + " ".join([str(utctime),
                                                             str(senders)]))
                    # choose the highest elevation
                    sender, elevation = max(senders, key=(lambda x: x[1]))

                except Empty:
                    continue

                if linepos is None:
                    linepos = compute_line_times(utctime, start_time, end_time)

                if(sat == satellite and
                   utctime >= start_time and
                   utctime < end_time and
                   utctime not in saved):
                    saved.append(utctime)

                    # getting line
                    logger.debug("requesting " +
                                 " ".join([str(satellite), str(utctime),
                                           str(sender), str(elevation)]))
                    host = sender.split(":")[0]
                    # TODO: this should be parallelized, and timed. I case of
                    # failure, another source should be used. Choking ?
                    line = self._requesters[host].get_line(satellite, utctime)

                    # compute line position in file
                    time_diff = utctime - start_time
                    time_diff = (time_diff.seconds
                                 + time_diff.microseconds / 1000000.0)
                    pos = LINE_SIZE * int(np.floor(time_diff *
                                                   LINES_PER_SECOND))
                    fp_.seek(pos, 0)
                    fp_.write(line)
                    self.send_lineinfo_to_server(satellite, utctime, elevation,
                                                 filename, pos)
                    # removing from line check list
                    linepos -= set([utctime])

                    delay = min(delay, datetime.utcnow() - utctime)
                    if len(linepos) > 0:
                        timethres = max(linepos) + CLIENT_TIMEOUT + delay
                    else:
                        timethres = datetime.utcnow()

            # shut down
            self.del_queue(queue)

    def send_lineinfo_to_server(self, *args, **kwargs):
        """Send information to our own server.
        """
        cfg = ConfigParser()
        cfg.read(self.cfgfile)
        host = cfg.get("local_reception", "localhost")
        host, port = (cfg.get(host, "hostname"),  cfg.get(host, "reqport"))
        del port
        self._requesters[host].send_lineinfo(*args, **kwargs)

    def stop(self):
        HaveBuffer.stop(self)
        self.loop = False
        for req in list(self._requesters.values()):
            req.stop()
