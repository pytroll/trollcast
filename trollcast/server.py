#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2015 Martin Raspaud

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

import logging
import os
import time
from ConfigParser import ConfigParser, NoOptionError
from datetime import datetime, timedelta
from fnmatch import fnmatch
from glob import glob
from threading import Event, Lock, Thread, Timer
from urlparse import urlparse

import numpy as np
from pyinotify import (IN_CLOSE_WRITE, IN_MODIFY, IN_OPEN, ProcessEvent,
                       ThreadedNotifier, WatchManager)
from zmq import (LINGER, NOBLOCK, POLLIN, PUB, PULL, ROUTER, SUB, SUBSCRIBE,
                 Poller, ZMQError)

from posttroll import context, strp_isoformat
from posttroll.message import Message
from pyorbital.orbital import Orbital
from trollcast import schedules
from trollcast.client import ZMQ_REQ_TIMEOUT, SimpleRequester
from trollsift.parser import globify, parse

logger = logging.getLogger(__name__)

subject = None
coords = None
tle_files = None


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
    dtype = np.dtype([('frame_sync', '>u1', (4, )),
                      ('version', '>u2'),
                      ('count', '>u1', (3,)),
                      ('replay', '>u1'),
                      ('data', 'V886'),
                      ('rscode', 'V128')])
    line_size = 1024
    cadu_sync_start = np.array([0x1A, 0xCF, 0xFC, 0x1D], dtype=np.uint8)
    satellites = {157: "NPP",
                  }

    def __init__(self, sat, reftime, fp):
        self.sat = sat
        self.fp = fp
        self.buffer = ''

    def read(self):
        self.buffer = self.buffer + self.fp.read()
        array = np.fromstring(self.buffer, dtype=self.dtype,
                              count=len(self.buffer) // self.line_size)
        for i, line in enumerate(array):
            platform_id = (line['version'] >> 6) & 0xff
            vcid = np.asscalar(line['version'] & 0x3f)

            count = np.asscalar(np.sum(line["count"] << (
                8 * np.array([2, 1, 0]))).astype(np.uint32))

            qual = 100
            if count == 0:
                qual = 0

            uid = vcid, count

            logger.debug("Got line VCID: " + str(vcid) + " count: " + str(count) + " platform: "
                         + str(platform_id))

            # yield ((self.sat, uid, 90, qual,
            #        data[self.line_size * i: self.line_size * (i + 1)]),
            #       self.line_size * (i + 1), None)
            yield {'platform_name': self.sat,
                   'uid': uid,
                   'data': self.buffer[self.line_size * i: self.line_size * (i + 1)],
                   'filepos': self.line_size * (i + 1),
                   'read_time': datetime.utcnow()}
        self.buffer = self.buffer[len(array) * self.line_size:]

# yield ((satellite, utctime, elevation, qual,
#                    data[self.line_size * i: self.line_size * (i + 1)]),
#                   self.line_size * (i + 1), f_elev)


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
                                     ("spare", '>u2'), ]),
                      ('back_scan', '>u2', (10, 3)),
                      ('space_data', '>u2', (10, 5)),
                      ('sync', '>u2'),
                      ('TIP_data', '>u2', (520, )),
                      ('spare', '>u2', (127, )),
                      ('image_data', '>u2', (2048, 5)),
                      ('aux_sync', '>u2', (100, ))])

    hrpt_sync = np.array([994, 1011, 437, 701, 644, 277, 452, 467, 833, 224,
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
        return timedelta(days=int(day / 2 - 1), milliseconds=int(msecs))

    def __init__(self, sat, reftime):
        self.sat = sat
        self.reftime = reftime
        self.count = 0
        self.time_threshold = timedelta(seconds=600)
        self.reftimes = []
        self.last_time = None
        self.lags = [0]

    def read(self, data, f_elev=None):
        """Read hrpt data.
        """

        # TODO:
        # - check next scheduled pass for time consistency
        # - check time diff between lines, and using a buffer fix diverging times.

        now = datetime.utcnow()
        year = now.year

        for i, line in enumerate(np.fromstring(data, dtype=self.dtype,
                                               count=len(data) / self.line_size)):
            days = self.timecode(line["timecode"])
            utctime = datetime(year, 1, 1) + days

            if utctime > now:
                # Can't have data from the future... yet :)
                utctime = datetime(year - 1, 1, 1) + days

            qual = (np.sum(line['aux_sync'] == self.hrpt_sync) +
                    np.sum(line['frame_sync'] == self.hrpt_sync_start))
            qual = 100 * (qual / 106.0)

            qual -= np.sum(abs(np.diff(line['image_data']
                                       [:, 4].astype(np.int16))) > 200)

            if np.sum(line['frame_sync'] == self.hrpt_sync_start) < 5:
                logger.info("Frame sync not in place, setting quality to 0")
                qual = 0

            # Take care of noisy time codes.

            new_time = datetime.now()

            # Check if we missed some scanlines
            if(self.last_time is not None):
                time_diff = (new_time - self.last_time)
                seconds = (time_diff.days * 24 * 60 * 60
                           + time_diff.seconds
                           + time_diff.microseconds / 1000000.0)
                self.lags.append(seconds)
                if(abs(utctime - (self.reftime +
                                  timedelta(seconds=self.count / 6.0 +
                                            max(self.lags))))
                   < timedelta(seconds=15)):
                    logger.debug(
                        "Looks like we lost some scanlines, adjusting %s counts.", seconds * 6.0)

                    self.count += int(max(self.lags) * 6.0)
                    self.time_threshold = max(timedelta(seconds=15),
                                              self.time_threshold)
                    self.reftimes = []
                    self.lags = [0]

            # Adjust reference time upon good quality scanlines
            if qual >= 99.9:
                # adjusting reftime:
                self.reftimes.append(
                    utctime - timedelta(seconds=self.count / 6.0))
                if (len(self.reftimes) > 50 and
                        self.time_threshold > timedelta(milliseconds=50)):
                    old_ref = self.reftime

                    hist, boundaries = np.histogram([time.mktime(reftime.timetuple())
                                                     for reftime in self.reftimes],
                                                    100)
                    idx = np.argmax(hist)
                    self.reftime = datetime.fromtimestamp(
                        (boundaries[idx] + boundaries[idx + 1]) / 2)
                    self.time_threshold = timedelta(milliseconds=50)
                    logger.info("Setting up new reftime %s (was %s)",
                                str(self.reftime),
                                str(old_ref))
            elif (abs(utctime - (self.reftime +
                                 timedelta(seconds=self.count / 6.0)))
                    > self.time_threshold):
                # Check if scanline is within expected range
                logger.debug("Spurious time for scanline %s (should be %s)",
                             str(utctime),
                             str((self.reftime + timedelta(seconds=self.count / 6.0))))
                qual = 0

            self.count += 1
            self.last_time = new_time

            qual = max(0, qual)

            logger.info("Quality " + str(qual))

            if qual != 100:
                logger.info("Degraded line: " + str(utctime))
                if f_elev is None:
                    satellite = "unknown"
                    yield ((satellite, utctime, None, qual,
                            data[self.line_size * i: self.line_size * (i + 1)]),
                           self.line_size * (i + 1), f_elev)
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
                    data[self.line_size * i: self.line_size * (i + 1)]),
                   self.line_size * (i + 1), f_elev)


FORMATS = {"HRPT": HRPT,
           "CADU": CADU}


class FileWatcher(Thread):

    def __init__(self, holder, uri, schedule_reader, datatype):
        Thread.__init__(self)
        self._loop = True
        self.datatype = datatype
        self._error_event = Event()
        self._wm = WatchManager()
        self._holder = holder
        self._uri = uri
        self._schedule_reader = schedule_reader
        self._notifier = ThreadedNotifier(self._wm,
                                          _EventHandler(self._holder,
                                                        self._uri,
                                                        self._schedule_reader,
                                                        self._error_event,
                                                        self.datatype))
        self._path, self._pattern = os.path.split(urlparse(uri).path)

    def start(self):
        """Start the file watcher
        """
        self._notifier.start()
        self._wm.add_watch(self._path, IN_OPEN | IN_CLOSE_WRITE | IN_MODIFY)
        Thread.start(self)

    def run(self):
        while self._loop:
            if self._error_event.wait(1):
                self._error_event.clear()
                self._notifier.stop()
                del self._notifier
                self._notifier = ThreadedNotifier(
                    self._wm,
                    _EventHandler(self._holder,
                                  self._uri,
                                  self._schedule_reader,
                                  self._error_event,
                                  self.datatype))
                self._notifier.start()
                self._wm.add_watch(self._path, IN_OPEN |
                                   IN_CLOSE_WRITE | IN_MODIFY)

    def stop(self):
        """Stop the file watcher
        """
        self._notifier.stop()
        self._loop = False


class _EventHandler(ProcessEvent):

    """Watch files
    """

    def __init__(self, holder, uri, schedule_reader, error_event, datatype):
        ProcessEvent.__init__(self)
        self._holder = holder
        self._uri = uri
        self._current_pass = None
        self._schedule_reader = schedule_reader
        self._loop = True
        self._error_event = error_event
        self.datatype = datatype

        self._path, self._pattern = os.path.split(urlparse(self._uri).path)

        self._readers = {}
        self._fp = None
        self._receiving = False
        self._timer = None
        self.sat = None
        self.time = None
        self.current_event = None
        self._pass_end_timer = None

        if self._schedule_reader.next_pass:
            next_pass_in = (self._schedule_reader.next_pass[0]
                            - datetime.utcnow())
            if next_pass_in.seconds > 0:
                self._timer = Timer(next_pass_in.seconds + 5,
                                    self.error)
                self._timer.start()

    def error(self):
        logger.critical("Reception expected but not started")
        self._error_event.set()

    def set_reception_active(self, event):
        self._receiving = True
        if self._timer is not None:
            self._timer.cancel()
        if self._pass_end_timer is not None:
            self._pass_end_timer.cancel()
        self._pass_end_timer = Timer(60, self.clean_up, event)

    def stop_receiving(self):
        self._receiving = False
        if self._schedule_reader.next_pass:
            next_pass_in = (self._schedule_reader.next_pass[0]
                            - datetime.utcnow())
            self._timer = Timer(next_pass_in.seconds + 5,
                                self.error)
            self._timer.start()

    def _reader(self, pathname, current_pass):
        """Read the file
        """
        try:
            try:
                filereader, position = self._readers[pathname]
            except KeyError:
                position = 0
                filetype = FORMATS[self.datatype]
                filereader = filetype(self.sat, self.time, self._fp)

            # self._fp.seek(position)

            #data = self._fp.read()

            # if position == 0:
            #    filetype = FORMATS[self.datatype]
            #    filereader = filetype(self.sat, self.time)

            # for elt, offset, f_elev in filereader.read(data):
            for item in filereader.read():
                self._readers[pathname] = filereader, position + \
                    item['filepos']
                if self.sat is not None:
                    if item['platform_name'] != self.sat:
                        logger.debug("Satellite id scrambled, "
                                     "lowering quality score.")
                        item['quality'] -= 1
                        item['platform_name'] = self.sat
                elif current_pass is not None:
                    if 'timecode' in item and item['timecode'] > current_pass[2] or item['timecode'] < current_pass[0]:
                        logger.debug("line %s doesn't match current pass %s",
                                     str(item['platform_name']) + '/' + str(item['uid']), str(current_pass))
                        continue
                    if item['platform_name'] != current_pass[1]:
                        logger.debug("Satellite id scrambled, "
                                     "lowering quality score.")
                        item['quality'] -= 1
                        item['platform_name'] = current_pass[1]
                elt = item.copy()
                elt.pop('filepos')
                yield elt
        except IOError, err:
            logger.warning("Can't read file: " + str(err))
            return

    def process_IN_OPEN(self, event):
        """When the file opens.
        """

        fname = os.path.basename(event.pathname)

        if not fnmatch(fname, globify(self._pattern)):
            logger.debug("Ignoring %s", event.pathname)
            return False

        if self.current_event is None:
            self.current_event = event
        elif(event.pathname != self.current_event.pathname):
            self.clean_up(self.current_event)
            self.current_event = event

        if self._fp is None:
            self._fp = open(event.pathname)
            self._current_pass = self._schedule_reader.next_pass
            info = parse(self._pattern, fname)
            try:
                self.sat = info["platform_name"]
                self.time = info["utctime"]
            except KeyError:
                logger.info("Could not retrieve satellite name from filename")

        self.set_reception_active(event)
        return self._fp is not None

    def process_IN_MODIFY(self, event):
        """File has been modified, read it !
        """

        if not self.process_IN_OPEN(event):
            return

        logger.debug("File modified! %s", event.pathname)

        fname = os.path.basename(event.pathname)

        if not fnmatch(fname, globify(self._pattern)):
            return

        self.set_reception_active(event)

        for item in self._reader(event.pathname, self._current_pass):
            # for sat, key, elevation, qual, data in self._reader(event.pathname,
            #                                                    self._current_pass):
            if item.get('quality', 100) > 0:
                self._holder.add(item)

    # def process_IN_CLOSE_WRITE(self, event):
    def clean_up(self, event):
        """Clean up.
        """
        fname = os.path.basename(event.pathname)

        if not fnmatch(fname, globify(self._pattern)):
            return

        if self._fp is not None:
            self._fp.close()
        else:
            logger.warning("File descriptor is None for %s", event.pathname)
        self._fp = None
        self._schedule_reader.get_next_pass()
        self.stop_receiving()
        try:
            del self._readers[event.pathname]
        except KeyError:
            logger.info("No reader defined for %s", str(event.pathname))



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

        logger.debug("Grabbing scanline from source")
        reqmsg = Message(subject,
                         'request',
                         {"type": "scanline",
                          "satellite": self._sat,
                          "utctime": self._key})
        rep = self._req.send_and_recv(str(reqmsg), ZMQ_REQ_TIMEOUT)

        if rep and rep.data:
            self._data = rep.data
            logger.debug("Retrieved scanline from source successfully")
        else:
            self._data = None
            logger.warning("Empty scanline from source!")
        return self._data

    def __str__(self):
        return str(self.get_data())

    def __add__(self, other):
        return str(self) + other

    def __radd__(self, other):
        return other + str(self)


class MirrorWatcher(Thread):

    """Watches another server.
    """

    def __init__(self, holder, host, pubport, reqport, sched):
        Thread.__init__(self)
        self._holder = holder
        self._pubaddress = "tcp://" + host + ":" + str(pubport)
        self._reqaddress = "tcp://" + host + ":" + str(reqport)

        self._req = SimpleRequester(host, reqport)

        self._subsocket = context.socket(SUB)
        self._subsocket.connect(self._pubaddress)
        self._subsocket.setsockopt(SUBSCRIBE, "pytroll")
        self._poller = Poller()
        self._poller.register(self._subsocket, POLLIN)
        self._lock = Lock()
        self._loop = True
        self._sched = sched

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
                key = strp_isoformat(message.data["uid"])
                elevation = message.data["elevation"]
                quality = message.data.get("quality", 100)
                data = _MirrorGetter(self._req, sat, key)
                self._holder.add(sat, key, elevation, quality, data)
            if message.type == "heartbeat":
                logger.debug("Got heartbeat from " + str(self._pubaddress)
                             + ": " + str(message))
                self._sched.mirror_next_pass = message.data["next_pass"]
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
            for key, val in satlines.items():
                if val['read_time'] < datetime.utcnow() - timedelta(hours=self._delay):
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
        return self.get(sat, key)['data']

    # def add(self, sat, key, elevation, qual, data):
    def add(self, item):
        """Add some data.
        """
        with self._lock:
            self._data.setdefault(item['platform_name'], {})[
                item['uid']] = item
        display_item = item.copy()
        display_item.pop('data')
        display_item.pop('read_time')
        logger.debug("Got stuff for " + str(display_item))
        self.have(display_item)

    def have(self, item):
        """Tell the world about our new data.
        """
        to_send = item.copy()
        to_send["satellite"] = item['platform_name']
        to_send["origin"] = self._origin

        msg = Message(subject, "have", to_send).encode()
        logger.debug('Publishing %s', str(msg))
        self._pub.send(msg)


class Publisher(object):

    """Publish stuff.
    """

    def __init__(self, port):
        self._socket = context.socket(PUB)
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


class ScheduleReader(object):

    """Reads and handles a schedule
    """

    def __init__(self, filename, fileformat):
        """

        Arguments:
        - `filename`: schedule filename
        - `fileformat`: schedule format
        """
        self._filename = filename
        self._fileformat = fileformat
        self.next_pass = None
        self.mirror_next_pass = None

    def get_next_pass(self):
        """Get the next pass from the schedule
        """
        try:
            fun = getattr(schedules, self._fileformat)
        except TypeError:
            return None
        logger.debug("using %s", str(fun))
        now = datetime.utcnow()
        for overpass in fun(self._filename):
            if overpass[2] > now:
                self.next_pass = overpass
                return overpass


class Heart(Thread):

    """Send heartbeats once in a while.
    """

    def __init__(self, pub, address, interval, schedule_reader):
        Thread.__init__(self)
        self._loop = True
        self._event = Event()
        self._address = address
        self._pub = pub
        self._schedule_reader = schedule_reader
        self._interval = interval

    def run(self):
        while self._loop:
            to_send = {}
            keys = ("start_time", "platform_name", "end_time")
            next_pass = self._schedule_reader.get_next_pass()
            mirror_next_pass = self._schedule_reader.mirror_next_pass
            passes = []
            if next_pass is not None:
                passes.append(dict(zip(keys, next_pass)))
            if mirror_next_pass is not None:
                passes.extend(mirror_next_pass)
            if passes:
                to_send["next_pass"] = sorted(
                    passes, key=(lambda x: x.get("start_time")))
            to_send["addr"] = self._address
            msg = Message(subject, "heartbeat", to_send).encode()
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

    def __init__(self, holder, req_port, rep_port, station):
        Thread.__init__(self)

        self._holder = holder
        self._loop = True
        self.req_port = req_port
        self.rep_port = rep_port
        self._station = station
        self._lock = Lock()
        self.req_socket = context.socket(PULL)
        self.req_socket.bind("tcp://*:" + str(self.req_port))
        self._poller = Poller()
        self._poller.register(self.req_socket, POLLIN)
        self._socket = context.socket(ROUTER)
        self._socket.bind("tcp://*:" + str(self.rep_port))

    def send(self, message, req_id, address):
        """Send a message
        """
        if message.binary:
            logger.debug("Response: " + " ".join(str(message).split()[:6]))
        else:
            logger.debug("Response: " + str(message))
        with self._lock:
            self._socket.send_multipart([address, req_id, str(message)])

    def pong(self):
        """Reply to ping
        """
        return Message(subject, "pong", {"station": self._station})

    def scanline(self, message):
        """Reply to scanline request
        """
        sat = message.data['platform_name']
        key = message.data["uid"]
        if isinstance(key, list):
            key = tuple(key)
        try:
            data = self._holder.get_data(sat, key)
        except KeyError:
            logger.exception("Can't find")
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
            if self.req_socket in socks and socks[self.req_socket] == POLLIN:
                logger.debug("Received a request")
                tic = datetime.utcnow()
                with self._lock:
                    address, req_id, payload = self.req_socket.recv_multipart(
                        NOBLOCK)
                message = Message(rawstr=payload)
                logger.debug("processing request: " + str(message))
                reply = Message(subject, "error")
                try:
                    # TODO: run in Threads
                    if message.type == "ping":
                        reply = self.pong()
                    elif (message.type == "request" and
                          message.data["type"] == "scanline"):
                        reply = self.scanline(message)
                    elif (message.type == "notice" and
                          message.data["type"] == "scanline"):
                        reply = self.notice(message)
                    else:  # unknown request
                        reply = self.unknown(message)
                except:
                    logger.exception("Something went wrong"
                                     " when processing the request:")
                finally:
                    self.send(reply, req_id, address)
                logger.debug("processed request in %s",
                             str(datetime.utcnow() - tic))
            else:  # timeout
                pass

    def stop(self):
        """Stop the request manager.
        """
        self._loop = False
        self.req_socket.setsockopt(LINGER, 0)
        self.req_socket.close()
        self._socket.setsockopt(LINGER, 0)
        self._socket.close()


def set_subject(station):
    global subject
    subject = '/oper/polar/direct_readout/' + station


def serve(configfile):
    """Serve forever.
    """

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
        pub = Publisher(pubport)

        # schedule reader
        try:
            sched = ScheduleReader(cfg.get("local_reception", "schedule_file"),
                                   cfg.get("local_reception", "schedule_format"))
            sched.get_next_pass()
        except NoOptionError:
            logger.warning("No schedule file given")
            sched = ScheduleReader(None, None)

        # heart
        hostname = cfg.get(host, "hostname")
        pubaddress = hostname + ":" + str(pubport)
        heart = Heart(pub, pubaddress, 30, sched)
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
            logger.warning(
                path + " doesn't exist, not getting data from files")
        else:
            pattern = cfg.get("local_reception", "file_pattern", raw=True)
            try:
                datatype = cfg.get("local_reception", "data_type", raw=True)
            except NoOptionError:
                datatype = "HRPT"
            watcher = FileWatcher(holder, os.path.join(
                path, pattern), sched, datatype)
            watcher.start()

        mirror_watcher = None
        try:
            mirror = cfg.get("local_reception", "mirror")
        except NoOptionError:
            pass
        else:
            pubport_m = cfg.getint(mirror, "pubport")
            reqport_m = cfg.getint(mirror, "reqport")
            host_m = cfg.get(mirror, "hostname")
            mirror_watcher = MirrorWatcher(holder,
                                           host_m, pubport_m, reqport_m,
                                           sched)
            mirror_watcher.start()

        # request manager
        reqport = cfg.getint(host, "reqport")
        repport = cfg.getint(host, "repport")
        reqman = RequestManager(holder, reqport, repport, station)
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

    # Test hrpt reading

    coords = [16.148649, 58.581844, 0.052765]

    reftime = datetime.strptime(
        os.path.basename(sys.argv[1])[:14], "%Y%m%d%H%M%S")
    sat = os.path.basename(sys.argv[1])[15:22].replace("_", " ")
    print "***", reftime, sat

    with open(sys.argv[1]) as fp:
        lines = fp.read()

    hrpt = HRPT(sat, reftime)

    for truc in hrpt.read(lines):
        print truc[0][:4]
        # raw_input()

    try:
        serve(sys.argv[1])
    except KeyboardInterrupt:
        print "ok, stopping"
