#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2012, 2013 SMHI

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

"""Trollcast, server side.

Trollcasting is loosely based on the bittorrent concepts, and adapted to
satellite data.

Limitations:
 - HRPT specific at the moment

TODO:
 - Include files from a library, not only the currently written file to the
   list of scanlines
 - Implement choking
 - de-hardcode filename
"""
from __future__ import with_statement 

import logging
import os
from ConfigParser import ConfigParser, NoOptionError
from datetime import datetime, timedelta
from fnmatch import fnmatch
from glob import glob
from threading import Thread, Lock, Event
from urlparse import urlparse, urlunparse

import numpy as np
import time
from posttroll import strp_isoformat
from posttroll.message import Message
from posttroll.subscriber import Subscriber
from pyorbital.orbital import Orbital
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from zmq import Context, Poller, LINGER, PUB, REP, REQ, POLLIN, NOBLOCK

logger = logging.getLogger(__name__)

LINE_SIZE = 11090 * 2

CACHE_SIZE = 32

HRPT_SYNC = np.array([ 994, 1011, 437, 701, 644, 277, 452, 467, 833, 224, 694,
        990, 220, 409, 1010, 403, 654, 105, 62, 867, 75, 149, 320, 725, 668,
        581, 866, 109, 166, 941, 1022, 59, 989, 182, 461, 197, 751, 359, 704,
        66, 387, 238, 850, 746, 473, 573, 282, 6, 212, 169, 623, 761, 979, 338,
        249, 448, 331, 911, 853, 536, 323, 703, 712, 370, 30, 900, 527, 977,
        286, 158, 26, 796, 705, 100, 432, 515, 633, 77, 65, 489, 186, 101, 406,
        560, 148, 358, 742, 113, 878, 453, 501, 882, 525, 925, 377, 324, 589,
        594, 496, 972], dtype=np.uint16)
HRPT_SYNC_START = np.array([644, 367, 860, 413, 527, 149], dtype=np.uint16)

SATELLITES = {7: "NOAA 15",
              3: "NOAA 16",
              13: "NOAA 18",
              15: "NOAA 19"}

def timecode(tc_array):
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

class Holder(object):

    def __init__(self, configfile):
        self._holder = {}
        cfg = ConfigParser()
        cfg.read(configfile)
        host = cfg.get("local_reception", "localhost")
        hostname = cfg.get(host, "hostname")
        port = cfg.get(host, "pubport")
        self._addr = hostname + ":" + port

        self._station = cfg.get("local_reception", "station")

        self._context = Context()
        self._socket = self._context.socket(PUB)
        self._socket.bind("tcp://*:" + port)
        self._lock = Lock()
        self._cache = []
        
        
    def __del__(self, *args, **kwargs):
        self._socket.close()

    def get(self, *args, **kwargs):
        return self._holder.get(*args, **kwargs)

    def __getitem__(self, *args, **kwargs):
        return self._holder.__getitem__(*args, **kwargs)
    
    def send_have(self, satellite, utctime, elevation):
        """Sends 'have' message for *satellite*, *utctime*, *elevation*.
        """
        to_send = {}
        to_send["satellite"] = satellite
        to_send["timecode"] = utctime.isoformat()
        to_send["elevation"] = elevation
        to_send["origin"] = self._addr
        msg = Message('/oper/polar/direct_readout/' + self._station, "have",
                      to_send).encode()
        self._socket.send(msg)

    def send_heartbeat(self, next_pass_time="unknown"):
        to_send = {}
        to_send["next_pass_time"] = next_pass_time
        to_send["addr"] = self._addr
        msg =  Message('/oper/polar/direct_readout/' + self._station,
                       "heartbeat", to_send).encode()
        logger.debug("sending heartbeat: " + str(msg))
        self._socket.send(msg)

    def get_scanline(self, satellite, utctime):
        info = self._holder[satellite][utctime]
        if len(info) == 4:
            return info[3]
        else:
            url = urlparse(self._holder[satellite][utctime][1])
            with open(url.path, "rb") as fp_:
                fp_.seek(self._holder[satellite][utctime][0])
                return fp_.read(LINE_SIZE)


        
    def add_scanline(self, satellite, utctime, elevation, line_start, filename, line=None):
        """Adds the scanline to the server. Typically used by the client to
        signal newly received lines.
        """
        self._lock.acquire()
        try:
            if(satellite not in self._holder or
               utctime not in self._holder[satellite]):
                if line:
                    self._holder.setdefault(satellite,
                                            {})[utctime] = (line_start,
                                                            filename,
                                                            elevation,
                                                            line)
                    self._cache.append((satellite, utctime))
                    while len(self._cache) > CACHE_SIZE:
                        sat, deltime = self._cache[0]
                        del self._cache[0]
                        self._holder[sat][deltime] = \
                                                self._holder[sat][deltime][:3]
                        
                else:
                    self._holder.setdefault(satellite,
                                            {})[utctime] = (line_start,
                                                            filename,
                                                            elevation)
                self.send_have(satellite, utctime, elevation)
        finally:
            self._lock.release()
        

class FileStreamer(FileSystemEventHandler):
    """Get the updates from files.

    TODO: separate holder from file handling.
    """
    def __init__(self, holder, configfile, *args, **kwargs):
        FileSystemEventHandler.__init__(self, *args, **kwargs)
        self._file = None
        self._filename = ""
        self._where = 0
        self._satellite = ""
        self._orbital = None
        cfg = ConfigParser()
        cfg.read(configfile)
        self._coords = cfg.get("local_reception", "coordinates").split(" ")
        self._coords = [float(self._coords[0]),
                        float(self._coords[1]),
                        float(self._coords[2])]
        self._station = cfg.get("local_reception", "station")
        logger.debug("Station " + self._station +
                     " located at: " + str(self._coords))
        try:
            self._tle_files = cfg.get("local_reception", "tle_files")
        except NoOptionError:
            self._tle_files = None

        self._file_pattern = cfg.get("local_reception", "file_pattern")
        
        self.scanlines = holder

        self._warn = True
        
    def update_satellite(self, satellite):
        """Update satellite and renew the orbital instance.
        """
        if satellite != self._satellite:
            self._satellite = satellite
            if self._tle_files is not None:
                filelist = glob(self._tle_files)
                tle_file = max(filelist, key=lambda x: os.stat(x).st_mtime)
            else:
                tle_file = None
            self._orbital = Orbital(self._satellite.upper(), tle_file)

    def on_created(self, event):
        """Callback when file is created.
        """
        if event.src_path != self._filename:
            if self._filename:
                logger.info("Closing: " + self._filename)
            if self._file:
                self._file.close()
            self._file = None
            self._filename = ""
            self._where = 0
            self._satellite = ""
            self._warn = True
        logger.info("New file detected: " + event.src_path)


    def on_opened(self, event):
        """Callback when file is opened
        """
        fname = os.path.split(event.src_path)[1]
        if self._file is None and fnmatch(fname, self._file_pattern):
            logger.info("File opened: " + event.src_path)
            self._filename = event.src_path
            self._file = open(event.src_path, "rb")
            self._where = 0
            self._satellite = ""

            self._orbital = None
            self._warn = True

    def on_modified(self, event):
        self.on_opened(event)

        if event.src_path != self._filename:
            return
            
        self._file.seek(self._where)
        line = self._file.read(LINE_SIZE)
        while len(line) == LINE_SIZE:
            line_start = self._where
            self._where = self._file.tell()
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


            array = np.fromstring(line, dtype=dtype)
            if np.all(abs(HRPT_SYNC_START - 
                          array["frame_sync"]) > 1):
                array = array.newbyteorder()
                
            satellite = SATELLITES[((array["id"]["id"] >> 3) & 15)[0]]
            self.update_satellite(satellite)

            
            # FIXME: this means server can only share 1 year of hrpt data.
            now = datetime.utcnow()
            year = now.year
            utctime = datetime(year, 1, 1) + timecode(array["timecode"][0])
            if utctime > now:
                # Can't have data from the future... yet :)
                utctime = (datetime(year - 1, 1, 1)
                           + timecode(array["timecode"][0]))

            # Check that we receive real-time data
            if not (np.all(array['aux_sync'] == HRPT_SYNC) and
                    np.all(array['frame_sync'] == HRPT_SYNC_START)):
                logger.info("Garbage line: " + str(utctime))
                line = self._file.read(LINE_SIZE)
                continue

            if (now - utctime).days > 7 and self._warn:
                logger.warning("Data is more than a week old: " + str(utctime))
                self._warn = False
                
            elevation = self._orbital.get_observer_look(utctime,
                                                        *self._coords)[1]
            logger.debug("Got line " + utctime.isoformat() + " "
                         + self._satellite + " "
                         + str(elevation))


            
            # TODO:
            # - serve also already present files
            # - timeout and close the file
            self.scanlines.add_scanline(self._satellite, utctime,
                                        elevation, line_start, self._filename,
                                        line)

            line = self._file.read(LINE_SIZE)

        self._file.seek(self._where)        

class MirrorStreamer(Thread):
    """Act as a relay...
    """

    def __init__(self, holder, configfile):
        Thread.__init__(self)
        
        self.scanlines = holder
        
        cfg = ConfigParser()
        cfg.read(configfile)
        host = cfg.get("local_reception", "mirror")
        hostname = cfg.get(host, "hostname")
        port = cfg.get(host, "pubport")
        rport = cfg.get(host, "reqport")
        address = "tcp://" + hostname + ":" + port
        self._sub = Subscriber([address], "hrpt 0")
        self._reqaddr = "tcp://" + hostname + ":" + rport

    def run(self):

        for message in self._sub.recv(1):
            if message is None:
                continue
            if(message.type == "have"):
                logger.debug("Relaying " + str(message.data["timecode"]))
                self.scanlines.add_scanline(message.data["satellite"],
                                            strp_isoformat(message.data["timecode"]),
                                            message.data["elevation"],
                                            None,
                                            self._reqaddr)

            if(message.type) == "heartbeat":
                self.scanlines._socket.send(str(message))
                
    def stop(self):
        """Stop streaming.
        """
        self._sub.stop()
        
class Looper(object):

    def __init__(self):
        self._loop = True

    def stop(self):
        self._loop = False

class Socket(object):
    def __init__(self, addr, stype):
        self._context = Context()
        self._socket = self._context.socket(stype)
        if stype in [REP, PUB]:
            self._socket.bind(addr)
        else:
            self._socket.connect(addr)

    def __del__(self, *args, **kwargs):
        self._socket.close()

class SocketLooper(Socket, Looper):
    
    def __init__(self, *args, **kwargs):
        Looper.__init__(self)
        Socket.__init__(self, *args, **kwargs)

class SocketLooperThread(SocketLooper, Thread):
    def __init__(self, *args, **kwargs):
        Thread.__init__(self)
        SocketLooper.__init__(self, *args, **kwargs)

class Heart(Thread):
    def __init__(self, holder, *args, **kwargs):
        Thread.__init__(self, *args, **kwargs)
        self.loop = True
        self.holder = holder
        self.event = Event()
        self.interval = 300

    def run(self):
        while self.loop:
            self.holder.send_heartbeat()
            self.event.wait(self.interval)

    def stop(self):
        self.loop = False
        self.event.set()

class Responder(SocketLooperThread):

    # TODO: this should not respond to everyone. It should check if the
    # requester is listed in the configuration file...
    
    def __init__(self, holder, configfile, *args, **kwargs):
        SocketLooperThread.__init__(self, *args, **kwargs)
        self._holder = holder
        self._loop = True

        cfg = ConfigParser()
        cfg.read(configfile)
        self._station = cfg.get("local_reception", "station")

        self.mirrors = {}


    def __del__(self, *args, **kwargs):
        self._socket.close()
        for mirror in self.mirrors.values():
            mirror.close()
            
        
    def forward_request(self, address, message):
        """Forward a request to another server.
        """
        if address not in self.mirrors:
            context = Context()
            socket = context.socket(REQ)
            socket.setsockopt(LINGER, 1)
            socket.connect(address)
            self.mirrors[address] = socket
        else:
            socket = self.mirrors[address]
        socket.send(str(message))
        return socket.recv()


    def run(self):
        poller = Poller()
        poller.register(self._socket, POLLIN)
        
        while self._loop:
            socks = dict(poller.poll(timeout=2))
            if self._socket in socks and socks[self._socket] == POLLIN:
                message = Message(rawstr=self._socket.recv(NOBLOCK))
                
                # send list of scanlines
                if(message.type == "request" and
                   message.data["type"] == "scanlines"):
                    sat = message.data["satellite"]
                    epoch = "1950-01-01T00:00:00"
                    start_time = strp_isoformat(message.data.get("start_time",
                                                                 epoch))
                    end_time = strp_isoformat(message.data.get("end_time",
                                                               epoch))

                    resp = Message('/oper/polar/direct_readout/' + self._station,
                                   "scanlines",
                                   [(utctime.isoformat(),
                                     self._holder[sat][utctime][2])
                                    for utctime in self._holder.get(sat, [])
                                    if utctime >= start_time
                                    and utctime <= end_time])
                    self._socket.send(str(resp))

                # send one scanline
                elif(message.type == "request" and
                     message.data["type"] == "scanline"):
                    sat = message.data["satellite"]
                    utctime = strp_isoformat(message.data["utctime"])
                    url = urlparse(self._holder[sat][utctime][1])
                    if url.scheme in ["", "file"]: # data is locally stored.
                        resp = Message('/oper/polar/direct_readout/'
                                       + self._station,
                                       "scanline",
                                       self._holder.get_scanline(sat, utctime),
                                       binary=True)
                    else: # it's the address of a remote server.
                        resp = self.forward_request(urlunparse(url),
                                                    message)
                    self._socket.send(str(resp))

                # take in a new scanline
                elif(message.type == "notice" and
                     message.data["type"] == "scanline"):
                    sat = message.data["satellite"]
                    utctime = message.data["utctime"]
                    elevation = message.data["elevation"]
                    filename = message.data["filename"]
                    line_start = message.data["file_position"]
                    utctime = strp_isoformat(utctime)
                    self._holder.add_scanline(sat, utctime, elevation,
                                              line_start, filename)
                    resp = Message('/oper/polar/direct_readout/'
                                       + self._station,
                                       "notice",
                                       "ack")
                    self._socket.send(str(resp))
                elif(message.type == "ping"):
                    resp = Message('/oper/polar/direct_readout/'
                                   + self._station,
                                   "pong",
                                   {"station": self._station})
                    self._socket.send(str(resp))
    def stop(self):
        self._loop = False


def serve(configfile):
    """Serve forever.
    """

    scanlines = Holder(configfile)
    heartbeat = Heart(scanlines)
    fstreamer = FileStreamer(scanlines, configfile)
    notifier = Observer()
    cfg = ConfigParser()
    cfg.read(configfile)
    path = cfg.get("local_reception", "data_dir")
    notifier.schedule(fstreamer, path, recursive=False)
    

    local_station = cfg.get("local_reception", "localhost")
    responder_port = cfg.get(local_station, "reqport")
    responder = Responder(scanlines, configfile,
                          "tcp://*:" + responder_port, REP)
    responder.start()
    heartbeat.start()
    mirror = None
    try:
        mirror = MirrorStreamer(scanlines, configfile)
        mirror.start()
    except NoOptionError:
        pass
    
    notifier.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        notifier.stop()
    
    responder.stop()
    heartbeat.stop()
    notifier.join()

    if mirror is not None:
        mirror.stop()
        
