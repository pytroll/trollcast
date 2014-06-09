#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 Martin Raspaud

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

"""New version of the trollcast server

TODO:
 - add lines when local client gets data (if missing)
 - check that mirror server is alive
"""

from ConfigParser import ConfigParser, NoOptionError
from zmq import Context, Poller, LINGER, PUB, REP, REQ, POLLIN, NOBLOCK, SUB, SUBSCRIBE, ZMQError
from threading import Thread, Event, Lock
from posttroll.message import Message
from pyorbital.orbital import Orbital
import logging
import time
from datetime import datetime, timedelta
from posttroll import strp_isoformat
from fnmatch import fnmatch
from pyinotify import (WatchManager, ProcessEvent, ThreadedNotifier,
                       IN_MODIFY, IN_OPEN, IN_CLOSE_WRITE)
from urlparse import urlparse
import os
import numpy as np
from glob import glob

logger = logging.getLogger(__name__)

def get_f_elev(satellite):
    """Get the elevation function for a given satellite
    """

    if tle_files is not None:
        filelist = glob(tle_files)
        tle_file = max(filelist, key=lambda x: os.stat(x).st_mtime)
    else:
        tle_file = None

    orb = Orbital(satellite.upper(), tle_file)
    def f_elev(utctime):
        """Get the elevation for the given *utctime*.
        """
        return orb.get_observer_look(utctime, *coords)[1]
    f_elev.satellite = satellite
    return f_elev


class CADU(object):
    """The cadu reader class
    """
    @staticmethod
    def is_it(data):
        return False

class HRPT(object):
    """The hrpt reader class
    """
    dtype = np.dtype([('frame_sync', '>u2', (6, )),
                      ('id', [('id', '>u2'),
                              ('spare', '>u2')]),
                              ('timecode', '>u2', (4, )),
                      ('telemetry', [("ramp_calibration", '>u2', (5, )),
                                     ("PRT", '>u2', (3, )),
                                     ("ch3_patch_temp", '>u2'),
                                     ("spare", '>u2'),]),
                      ('back_scan', '>u2', (10, 3)),
                      ('space_data', '>u2', (10, 5)),
                      ('sync', '>u2'),
                      ('TIP_data', '>u2', (520, )),
                      ('spare', '>u2', (127, )),
                      ('image_data', '>u2', (2048, 5)),
                      ('aux_sync', '>u2', (100, ))])

    hrpt_sync = np.array([ 994, 1011, 437, 701, 644, 277, 452, 467, 833, 224,
                           694, 990, 220, 409, 1010, 403, 654, 105, 62, 867,
                           75, 149, 320, 725, 668, 581, 866, 109, 166, 941,
                           1022, 59, 989, 182, 461, 197, 751, 359, 704, 66,
                           387, 238, 850, 746, 473, 573, 282, 6, 212, 169, 623,
                           761, 979, 338, 249, 448, 331, 911, 853, 536, 323,
                           703, 712, 370, 30, 900, 527, 977, 286, 158, 26, 796,
                           705, 100, 432, 515, 633, 77, 65, 489, 186, 101, 406,
                           560, 148, 358, 742, 113, 878, 453, 501, 882, 525,
                           925, 377, 324, 589, 594, 496, 972], dtype=np.uint16)

    hrpt_sync_start = np.array([644, 367, 860, 413, 527, 149], dtype=np.uint16)

    satellites = {7: "NOAA 15",
                  3: "NOAA 16",
                  13: "NOAA 18",
                  15: "NOAA 19"}

    line_size = 11090 * 2

    @staticmethod
    def is_it(data):
        return True

    @staticmethod
    def timecode(tc_array):
        """HRPT timecode reading
        """
        word = tc_array[0]
        day = word
        word = tc_array[1]
        msecs = ((127) & word) * 1024
        word = tc_array[2]
        msecs += word & 1023
        msecs *= 1024
        word = tc_array[3]
        msecs += word & 1023
        return timedelta(days=int(day/2 - 1), milliseconds=int(msecs))

    def read(self, data, f_elev=None):
        """Read hrpt data.
        """

        now = datetime.utcnow()
        year = now.year

        for i, line in enumerate(np.fromstring(data, dtype=self.dtype,
                                               count=len(data)/self.line_size)):

            days = self.timecode(line["timecode"])
            utctime = datetime(year, 1, 1) + days
            if utctime > now:
                # Can't have data from the future... yet :)
                utctime = datetime(year - 1, 1, 1) + days

            qual = (np.sum(line['aux_sync'] == self.hrpt_sync) +
                    np.sum(line['frame_sync'] == self.hrpt_sync_start))
            qual = (100*qual)/106
            logger.info("Quality " + str(qual))

            if qual != 100:
                logger.info("Garbage line: " + str(utctime))
                if f_elev is None:
                    satellite = "unknown"
                    yield ((satellite, utctime, None, qual,
                            data[self.line_size * i: self.line_size * (i+1)]),
                           self.line_size * (i+1), f_elev)
                    continue
                else:
                    satellite = f_elev.satellite
            else:
                try:
                    satellite = self.satellites[((line["id"]["id"] >> 3) & 15)]
                except KeyError:
                    satellite = "unknown"

            if f_elev is None:
                if satellite != "unknown":
                    f_elev = get_f_elev(satellite)
                    elevation = f_elev(utctime)
                else:
                    elevation = -180
            else:
                elevation = f_elev(utctime)


            logger.debug("Got line " + utctime.isoformat() + " "
                         + satellite + " "
                         + str(elevation))


            # TODO:
            # - serve also already present files
            # - timeout and close the file

            yield ((satellite, utctime, elevation, qual,
                    data[self.line_size * i: self.line_size * (i+1)]),
                   self.line_size * (i+1), f_elev)



FORMATS = [CADU, HRPT]

#from watchdog.events import FileSystemEventHandler
#from watchdog.observers import Observer

# class WDFileWatcher(FileSystemEventHandler):
#     """Watch files
#     """
#     def __init__(self, holder, uri):
#         FileSystemEventHandler.__init__(self)
#         self._holder = holder
#         self._uri = uri
#         self._loop = True
#         self._notifier = Observer()
#         self._path, self._pattern = os.path.split(urlparse(self._uri).path)
#         self._notifier.schedule(self, self._path, recursive=False)
#         self._readers = {}

#     def _reader(self, pathname):
#         """Read the file
#         """
#         # FIXME: the _readers dict has to be cleaned up !!
#         # FIXME: don't open the file each time.
#         try:
#             with open(pathname) as fp_:
#                 try:
#                     filereader, position, f_elev = self._readers[pathname]
#                 except KeyError:
#                     position = 0
#                     f_elev = None
#                 else:
#                     fp_.seek(position)

#                 data = fp_.read()

#                 if position == 0:
#                     for filetype in FORMATS:
#                         if filetype.is_it(data):
#                             filereader = filetype()

#                 for elt, offset, f_elev in filereader.read(data, f_elev):
#                     self._readers[pathname] = filereader, position + offset, f_elev
#                     yield elt
#         except IOError, e:
#             logger.warning("Can't read file: " + str(e))
#             return


#     def start(self):
#         """Start the file watcher
#         """
#         self._notifier.start()

#     def stop(self):
#         """Stop the file watcher
#         """
#         self._notifier.stop()

#     def on_modified(self, event):
#         path, fname = os.path.split(event.src_path)
#         del path
#         if not fnmatch(fname, self._pattern):
#             return

#         for sat, key, elevation, qual, data in self._reader(event.src_path):
#             self._holder.add(sat, key, elevation, qual, data)

class FileWatcher(object):

    def __init__(self, holder, uri):

        self._wm = WatchManager()
        self._notifier = ThreadedNotifier(self._wm, _EventHandler(holder, uri))
        self._path, self._pattern = os.path.split(urlparse(uri).path)

    def start(self):
        """Start the file watcher
        """
        self._notifier.start()
        self._wm.add_watch(self._path, IN_OPEN | IN_CLOSE_WRITE | IN_MODIFY)

    def stop(self):
        """Stop the file watcher
        """
        self._notifier.stop()


class _EventHandler(ProcessEvent):
    """Watch files
    """
    def __init__(self, holder, uri):
        ProcessEvent.__init__(self)
        self._holder = holder
        self._uri = uri
        self._loop = True

        self._path, self._pattern = os.path.split(urlparse(self._uri).path)

        self._readers = {}
        self._fp = None

    def _reader(self, pathname):
        """Read the file
        """
        try:
            try:
                filereader, position, f_elev = self._readers[pathname]
            except KeyError:
                position = 0
                f_elev = None
            else:
                self._fp.seek(position)

            data = self._fp.read()

            if position == 0:
                for filetype in FORMATS:
                    if filetype.is_it(data):
                        filereader = filetype()

            for elt, offset, f_elev in filereader.read(data, f_elev):
                self._readers[pathname] = filereader, position + offset, f_elev
                yield elt
        except IOError, err:
            logger.warning("Can't read file: " + str(err))
            return


    def process_IN_OPEN(self, event):
        """When the file opens.
        """

        fname = os.path.basename(event.pathname)

        if self._fp is None and fnmatch(fname, self._pattern):
            self._fp = open(event.pathname)

        return self._fp is not None

    def process_IN_MODIFY(self, event):
        """File has been modified, read it !
        """

        if not self.process_IN_OPEN(event):
            return

        fname = os.path.basename(event.pathname)

        if not fnmatch(fname, self._pattern):
            return

        for sat, key, elevation, qual, data in self._reader(event.pathname):
            self._holder.add(sat, key, elevation, qual, data)

    def process_IN_CLOSE_WRITE(self, event):
        """Clean up.
        """
        self._fp.close()
        self._fp = None
        del self._readers[event.pathname]


class _OldMirrorGetter(object):
    """Gets data from the mirror when needed.
    """

    def __init__(self, socket, sat, key, lock):
        self._socket = socket
        self._sat = sat
        self._key = key
        self._lock = lock
        self._data = None

    def get_data(self):
        """Get the actual data from the server we're mirroring
        """
        if self._data is not None:
            return self._data

        logger.debug("Grabbing scanline from mirror")
        req = Message(subject,
                      'request',
                      {"type": "scanline",
                       "satellite": self._sat,
                       "utctime": self._key})
        with self._lock:
            self._socket.send(str(req))
            rep = Message.decode(self._socket.recv())
        # FIXME: check that there actually is data there.
        self._data = rep.data
        logger.debug("Retrieved scanline from mirror successfully")
        return self._data

    def __str__(self):
        return self.get_data()

    def __add__(self, other):
        return str(self) + other

    def __radd__(self, other):
        return other + str(self)

class OldMirrorWatcher(Thread):
    """Watches a other server.
    """

    def __init__(self, holder, context, host, pubport, reqport):
        Thread.__init__(self)
        self._holder = holder
        self._pubaddress = "tcp://" + host + ":" + str(pubport)
        self._reqaddress = "tcp://" + host + ":" + str(reqport)

        self._reqsocket = context.socket(REQ)
        self._reqsocket.connect(self._reqaddress)

        self._subsocket = context.socket(SUB)
        self._subsocket.setsockopt(SUBSCRIBE, "pytroll")
        self._subsocket.connect(self._pubaddress)
        self._lock = Lock()
        self._loop = True

    def run(self):
        while self._loop:
            message = Message.decode(self._subsocket.recv())
            if message.type == "have":
                sat = message.data["satellite"]
                key = strp_isoformat(message.data["timecode"])
                elevation = message.data["elevation"]
                quality = message.data.get("quality", 100)
                data = _OldMirrorGetter(self._reqsocket,
                                     sat, key,
                                     self._lock)
                self._holder.add(sat, key, elevation, quality, data)
            if message.type == "heartbeat":
                logger.debug("Got heartbeat from " + str(self._pubaddress)
                             + ": " + str(message))

    def stop(self):
        """Stop the watcher
        """
        self._loop = False
        self._reqsocket.setsockopt(LINGER, 0)
        self._reqsocket.close()
        self._subsocket.setsockopt(LINGER, 0)
        self._subsocket.close()

from trollcast.client import SimpleRequester

class _MirrorGetter(object):
    """Gets data from the mirror when needed.
    """

    def __init__(self, req, sat, key):
        self._req = req
        self._sat = sat
        self._key = key
        self._data = None

    def get_data(self):
        """Get the actual data from the server we're mirroring
        """
        if self._data is not None:
            return self._data

        logger.debug("Grabbing scanline from mirror")
        reqmsg = Message(subject,
                         'request',
                         {"type": "scanline",
                          "satellite": self._sat,
                          "utctime": self._key})
        rep = self._req.send_and_recv(str(reqmsg), 300)
        # FIXME: check that there actually is data there.
        self._data = rep.data
        logger.debug("Retrieved scanline from mirror successfully")
        return self._data

    def __str__(self):
        return self.get_data()

    def __add__(self, other):
        return str(self) + other

    def __radd__(self, other):
        return other + str(self)

class MirrorWatcher(Thread):
    """Watches a other server.
    """

    def __init__(self, holder, context, host, pubport, reqport):
        Thread.__init__(self)
        self._holder = holder
        self._pubaddress = "tcp://" + host + ":" + str(pubport)
        self._reqaddress = "tcp://" + host + ":" + str(reqport)

        self._req = SimpleRequester(host, reqport, context)

        self._subsocket = context.socket(SUB)
        self._subsocket.connect(self._pubaddress)
        self._subsocket.setsockopt(SUBSCRIBE, "pytroll")
        self._poller = Poller()
        self._poller.register(self._subsocket, POLLIN)
        self._lock = Lock()
        self._loop = True

    def run(self):
        last_hb = datetime.now()
        minutes = 2
        while self._loop:
            if datetime.now() - last_hb > timedelta(minutes=minutes):
                logger.error("No heartbeat from " + str(self._pubaddress))
                last_hb = datetime.now()
                minutes = 1440
            socks = dict(self._poller.poll(2000))
            if (socks and
                self._subsocket in socks and
                socks[self._subsocket] == POLLIN):
                message = Message.decode(self._subsocket.recv())
            else:
                continue
            if message.type == "have":
                sat = message.data["satellite"]
                key = strp_isoformat(message.data["timecode"])
                elevation = message.data["elevation"]
                quality = message.data.get("quality", 100)
                data = _MirrorGetter(self._req, sat, key)
                self._holder.add(sat, key, elevation, quality, data)
            if message.type == "heartbeat":
                logger.debug("Got heartbeat from " + str(self._pubaddress)
                             + ": " + str(message))
                last_hb = datetime.now()
                minutes = 2

    def stop(self):
        """Stop the watcher
        """
        self._loop = False
        self._req.stop()
        self._subsocket.setsockopt(LINGER, 0)
        self._subsocket.close()


class DummyWatcher(Thread):
    """Dummy watcher for test purposes
    """
    def __init__(self, holder, uri):
        Thread.__init__(self)
        self._holder = holder
        self._uri = uri
        self._loop = True
        self._event = Event()

    def run(self):
        while self._loop:
            self._holder.add("NOAA 17", datetime.utcnow(),
                             18, 100, "dummy data")
            self._event.wait(self._uri)

    def stop(self):
        """Stop adding stuff
        """
        self._loop = False
        self._event.set()

class Cleaner(Thread):
    """Dummy watcher for test purposes
    """
    def __init__(self, holder, delay):
        Thread.__init__(self)
        self._holder = holder
        self._interval = 60
        self._delay = delay
        self._loop = True
        self._event = Event()

    def clean(self):
        """Clean the db
        """
        logger.debug("Cleaning")
        for sat in self._holder.sats():
            satlines = self._holder.get_sat(sat)
            for key in sorted(satlines):
                if key < datetime.utcnow() - timedelta(hours=self._delay):
                    self._holder.delete(sat, key)



    def run(self):
        while self._loop:
            self.clean()
            self._event.wait(self._interval)

    def stop(self):
        """Stop adding stuff
        """
        self._loop = False
        self._event.set()

class Holder(object):
    """The mighty data holder
    """

    def __init__(self, pub, origin):
        self._data = {}
        self._pub = pub
        self._origin = origin
        self._lock = Lock()

    def delete(self, sat, key):
        """Delete item
        """
        logger.debug("Removing from memory: " + str((sat, key)))
        with self._lock:
            del self._data[sat][key]

    def get_sat(self, sat):
        """Get the data for a given satellite *sat*.
        """
        return self._data[sat]

    def sats(self):
        """return the satellites in store.
        """
        return self._data.keys()

    def get(self, sat, key):
        """get the value of *sat* and *key*
        """
        with self._lock:
            return self._data[sat][key]

    def get_data(self, sat, key):
        """get the data of *sat* and *key*
        """
        return self.get(sat, key)[2]

    def add(self, sat, key, elevation, qual, data):
        """Add some data.
        """
        with self._lock:
            self._data.setdefault(sat, {})[key] = elevation, qual, data
        logger.debug("Got stuff for " + str((sat, key, elevation, qual)))
        self.have(sat, key, elevation, qual)

    def have(self, sat, key, elevation, qual):
        """Tell the world about our new data.
        """
        to_send = {}
        to_send["satellite"] = sat
        to_send["timecode"] = key
        to_send["elevation"] = elevation
        to_send["quality"] = qual
        to_send["origin"] = self._origin
        msg = Message(subject, "have", to_send).encode()
        self._pub.send(msg)

class Publisher(object):
    """Publish stuff.
    """
    def __init__(self, context, port):
        self._context = context
        self._socket = self._context.socket(PUB)
        self._socket.bind("tcp://*:" + str(port))
        self._lock = Lock()

    def send(self, message):
        """Publish something
        """
        with self._lock:
            self._socket.send(str(message))

    def stop(self):
        """Stop publishing.
        """
        with self._lock:
            self._socket.setsockopt(LINGER, 0)
            self._socket.close()

class Heart(Thread):
    """Send heartbeats once in a while.
    """

    def __init__(self, pub, address, interval):
        Thread.__init__(self)
        self._loop = True
        self._event = Event()
        self._address = address
        self._pub = pub
        self._interval = interval

    def run(self):
        while self._loop:
            to_send = {}
            to_send["next_pass_time"] = "unknown"
            to_send["addr"] = self._address
            msg =  Message(subject, "heartbeat", to_send).encode()
            logger.debug("sending heartbeat: " + str(msg))
            self._pub.send(msg)
            self._event.wait(self._interval)

    def stop(self):
        """Cardiac arrest
        """
        self._loop = False
        self._event.set()

class RequestManager(Thread):
    """Manage requests.
    """

    def __init__(self, holder, context, port, station):
        Thread.__init__(self)

        self._holder = holder
        self._loop = True
        self._port = port
        self._station = station
        self._lock = Lock()
        self._socket = context.socket(REP)
        self._socket.bind("tcp://*:" + str(self._port))
        self._poller = Poller()
        self._poller.register(self._socket, POLLIN)

    def send(self, message):
        """Send a message
        """
        if message.binary:
            logger.debug("Response: " + " ".join(str(message).split()[:6]))
        else:
            logger.debug("Response: " + str(message))
        self._socket.send(str(message))

    def pong(self):
        """Reply to ping
        """
        return Message(subject, "pong", {"station": self._station})

    def scanline(self, message):
        """Reply to scanline request
        """
        sat = message.data["satellite"]
        key = strp_isoformat(message.data["utctime"])
        try:
            data = self._holder.get_data(sat, key)
        except KeyError:
            resp = Message(subject, "missing")
        else:
            resp = Message(subject, "scanline", data, binary=True)
        return resp

    def notice(self, message):
        """Reply to notice message
        """
        del message
        return Message(subject, "ack")

    def unknown(self, message):
        """Reply to any unknown request.
        """
        del message
        return Message(subject, "unknown")

    def run(self):
        while self._loop:
            try:
                socks = dict(self._poller.poll(timeout=2000))
            except ZMQError:
                logger.info("Poller interrupted.")
                continue
            if self._socket in socks and socks[self._socket] == POLLIN:
                logger.debug("Received a request, waiting for the lock")
                with self._lock:
                    message = Message(rawstr=self._socket.recv(NOBLOCK))
                    logger.debug("processing request: " + str(message))
                    reply = Message(subject, "error")
                    try:
                        if message.type == "ping":
                            reply = self.pong()
                        elif (message.type == "request" and
                            message.data["type"] == "scanline"):
                            reply = self.scanline(message)
                        elif (message.type == "notice" and
                              message.data["type"] == "scanline"):
                            reply = self.notice(message)
                        else: # unknown request
                            reply = self.unknown(message)
                    finally:
                        self.send(reply)
            else: # timeout
                pass

    def stop(self):
        """Stop the request manager.
        """
        self._loop = False
        self._socket.setsockopt(LINGER, 0)
        self._socket.close()

def set_subject(station):
    global subject
    subject = '/oper/polar/direct_readout/' + station

def serve(configfile):
    """Serve forever.
    """

    context = Context()

    try:
        cfg = ConfigParser()
        cfg.read(configfile)

        host = cfg.get("local_reception", "localhost")

        # for messages
        station = cfg.get("local_reception", "station")
        set_subject(station)

        # for elevation
        global coords
        coords = cfg.get("local_reception", "coordinates")
        coords = [float(coord) for coord in coords.split()]

        global tle_files

        try:
            tle_files = cfg.get("local_reception", "tle_files")
        except NoOptionError:
            tle_files = None

        # publisher
        pubport = cfg.getint(host, "pubport")
        pub = Publisher(context, pubport)

        # heart
        hostname = cfg.get(host, "hostname")
        pubaddress = hostname + ":" + str(pubport)
        heart = Heart(pub, pubaddress, 30)
        heart.start()

        # holder
        holder = Holder(pub, pubaddress)

        # cleaner

        cleaner = Cleaner(holder, 1)
        cleaner.start()

        # watcher
        #watcher = DummyWatcher(holder, 2)
        path = cfg.get("local_reception", "data_dir")
        watcher = None

        if not os.path.exists(path):
            logger.warning(path + " doesn't exist, not getting data from files")
        else:
            pattern = cfg.get("local_reception", "file_pattern")
            watcher = FileWatcher(holder, os.path.join(path, pattern))
            watcher.start()

        mirror_watcher = None
        try:
            mirror = cfg.get("local_reception", "mirror")
        except NoOptionError:
            pass
        else:
            pubport_m = cfg.getint(mirror, "pubport")
            reqport_m = cfg.getint(mirror, "reqport")
            mirror_watcher = MirrorWatcher(holder, context,
                                           mirror, pubport_m, reqport_m)
            mirror_watcher.start()

        # request manager
        reqport = cfg.getint(host, "reqport")
        reqman = RequestManager(holder, context, reqport, station)
        reqman.start()

        while True:
            time.sleep(10000)

    except KeyboardInterrupt:
        pass
    except:
        logger.exception("There was an error!")
        raise
    finally:
        try:
            reqman.stop()
        except UnboundLocalError:
            pass

        try:
            if mirror_watcher is not None:
                mirror_watcher.stop()
        except UnboundLocalError:
            pass

        try:
            if watcher is not None:
                watcher.stop()
        except UnboundLocalError:
            pass
        try:
            cleaner.stop()
        except UnboundLocalError:
            pass
        try:
            heart.stop()
        except UnboundLocalError:
            pass
        try:
            pub.stop()
        except UnboundLocalError:
            pass
        try:
            context.term()
        except ZMQError:
            pass



if __name__ == '__main__':
    import sys
    ch1 = logging.StreamHandler()
    ch1.setLevel(logging.DEBUG)

    formatter = logging.Formatter('[%(levelname)s %(name)s %(asctime)s] '
                                  '%(message)s')
    ch1.setFormatter(formatter)

    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('').addHandler(ch1)
    logger = logging.getLogger("trollcast_server")

    try:
        serve(sys.argv[1])
    except KeyboardInterrupt:
        print "ok, stopping"



