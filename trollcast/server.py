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
weather satellite data.

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
from threading import Thread, Lock
from urlparse import urlparse, urlunparse
import socket
import numpy as np
import time
from posttroll import strp_isoformat
from posttroll.message import Message
from posttroll.subscriber import Subscriber
from pyorbital.orbital import Orbital
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from zmq import Context, Poller, LINGER, PUB, REP, REQ, POLLIN, NOBLOCK

from trollcast.formats.cadu import CADUReader
from trollcast.formats.hrpt import HRPTReader

readers = {"cadu": CADUReader,
           "hrpt": HRPTReader}

logger = logging.getLogger(__name__)

CACHE_SIZE = 320000000

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
        try:
            to_send["timecode"] = utctime.isoformat()
        except AttributeError:
            to_send["timecode"] = utctime[0], utctime[1], utctime[2].isoformat()
        to_send["elevation"] = elevation
        to_send["origin"] = self._addr
        msg = Message('/oper/polar/direct_readout/' + self._station, "have",
                      to_send).encode()
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

class SocketStreamer(Thread):
    """Get the updates from a socket.
    """

    def __init__(self, holder, configfile, *args, **kwargs):
        Thread.__init__(self, *args, **kwargs)
        self.loop = True
        self._filename = ""
        self._satellite = ""
        self.configfile = configfile
        cfg = ConfigParser()
        cfg.read(self.configfile)
        url = urlparse(cfg.get("local_reception", "url"))

        if url.scheme == "tcp":
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        elif url.scheme == "udp":
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        else:
            raise NotImplementedError("Only support tcp and udp for socket streams")
        host, port = url.netloc.split(":")
        if host == "localhost":
            host = ""

        self.scanlines = holder
        self._socket.bind((host, int(port)))
        self._socket.listen(1)
        self.conn, self.addr = self._socket.accept()
        self.reader = readers[cfg.get("local_reception", "data")]()
        self._orbital = None
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

    def update_satellite(self, satellite):
        if satellite != self._satellite:
            self._satellite = satellite
            if self._tle_files is not None:
                filelist = glob(self._tle_files)
                tle_file = max(filelist, key=lambda x: os.stat(x).st_mtime)
            else:
                tle_file = None
            self._orbital = Orbital(self._satellite.upper(), tle_file)

    def run(self):
        buff = ""
        line_start = 0
        while self.loop:
            #logger.debug("next segment")
            buff += self.conn.recv(1024)
            #print ":".join("{0:x}".format(ord(c)) for c in buff)
            try:
                res = None
                while len(buff) >= 1024:
                    #logger.debug("call from server")
                    res = self.reader.read_line(buff[:1024])
                    buff = buff[1024:]
                    if res is None:
                        break
                    uid, satellite, line = res
                    self.update_satellite(satellite)
                if res is None:
                    continue
            except ValueError:
                logger.exception()
                continue
            print uid
            elevation = self._orbital.get_observer_look(uid,
                                                        *self._coords)[1]
            logger.debug("Got packet " + str(uid) + " "
                         + satellite + " "
                         + str(elevation))
            self.scanlines.add_scanline(self._satellite, uid,
                                        elevation, line_start, self._filename,
                                        line)
            line_start += len(line)
            
    def stop(self):
        self.loop = False

class FileStreamer(object):
    """Get the updates from files.
    """

    def __init__(self, holder, configfile, *args, **kwargs):
        self.configfile = configfile
        self.holder = holder
        self.notifier = Observer()
        cfg = ConfigParser()
        cfg.read(configfile)
        self.reader = readers[cfg.get("local_reception", "data")]()
        self.fstreamer = _FileStreamer(holder, configfile, self.reader,
                                       *args, **kwargs)
        self.url = urlparse(cfg.get("local_reception", "url"))
        if self.url.scheme not in ["file", ""]:
            raise NotImplementedError("No support for scheme " +
                                      str(self.url.scheme))
        self.path = self.url.path

    def start(self):
        self.notifier.schedule(self.fstreamer, os.path.dirname(self.path),
                               recursive=False)
        self.notifier.start()
    
    def stop(self):
        self.notifier.stop()

    def join(self):
        self.notifier.join()
        
class _FileStreamer(FileSystemEventHandler):
    """Get the updates from files.

    TODO: separate holder from file handling.
    """
    def __init__(self, holder, configfile, reader, *args, **kwargs):
        FileSystemEventHandler.__init__(self, *args, **kwargs)
        self._reader = reader
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

        self.url = urlparse(cfg.get("local_reception", "url"))
        self._file_pattern = os.path.basename(self.url.path)
        
        self.scanlines = holder

    def update_satellite(self, satellite):
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
        logger.info("New file detected: " + event.src_path)


    def on_opened(self, event):
        """Callback when file is opened
        """
        fname = os.path.basename(event.src_path)
        if self._file is None and fnmatch(fname, self._file_pattern):
            logger.info("File opened: " + event.src_path)
            self._filename = event.src_path
            self._file = open(event.src_path, "rb")
            self._where = 0
            self._satellite = ""

    def on_modified(self, event):
        self.on_opened(event)

        if event.src_path != self._filename:
            return
            
        self._file.seek(self._where)
        line = self._file.read(self._reader.line_size)
        while len(line) >= self._reader.line_size:
            line_start = self._where
            self._where = self._file.tell()
            try:
                utctime, satellite, newline = self._reader.read_line(line[:self._reader.line_size])
            except TypeError:
                continue
            finally:
                line = line[self._reader.line_size:]
                
            uid = utctime
            if isinstance(utctime, (tuple, list)):
                utctime = uid[-1]
                
            self.update_satellite(satellite)
            
            elevation = self._orbital.get_observer_look(utctime, *self._coords)[1]
            logger.debug("Got line " + utctime.isoformat() + " "
                         + self._satellite + " "
                         + str(elevation))

            
            # TODO:
            # - serve also already present files
            # - timeout and close the file
            self.scanlines.add_scanline(satellite, uid,
                                        elevation, line_start, self._filename,
                                        newline)

            line = self._file.read(self._reader.line_size)

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
                    utctime = message.data["utctime"]
                    if isinstance(utctime, list):
                        utctime = tuple(utctime)
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
                
    def stop(self):
        self._loop = False


def serve(configfile):
    """Serve forever.
    """

    scanlines = Holder(configfile)
    
    streamer = None
    try:
        streamer = MirrorStreamer(scanlines, configfile)
    except NoOptionError:
        try:
            streamer = SocketStreamer(scanlines, configfile)
            logger.debug("Getting data from a socket")
        except NotImplementedError:
            streamer = FileStreamer(scanlines, configfile)
            logger.debug("Getting data from a file")
            
    cfg = ConfigParser()
    cfg.read(configfile)

    local_station = cfg.get("local_reception", "localhost")
    responder_port = cfg.get(local_station, "reqport")
    responder = Responder(scanlines, configfile,
                          "tcp://*:" + responder_port, REP)
    responder.start()
    streamer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        streamer.stop()
        responder.stop()

        
