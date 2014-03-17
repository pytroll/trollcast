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

"""Test the new server
"""


import unittest
from mock import MagicMock, patch
from threading import Thread

import sys
sys.modules["ConfigParser"] = MagicMock()
sys.modules["zmq"] = MagicMock()
sys.modules["posttroll.message"] = MagicMock()
sys.modules["pyorbital.orbital"] = MagicMock()
from trollcast import server
import time
import random

server.set_subject("bla")

class TestHeart(unittest.TestCase):

    def setUp(self):
        self.pub = MagicMock()

    def test_heart(self):
        """Test beating
        """
        
        heart = server.Heart(self.pub, address="hej", interval=0.15)
        heart.start()
        time.sleep(0.1)
        heart.stop()
        time.sleep(0.1)
        msg = self.pub.send.call_args[0][0]
        self.assertTrue(msg.startswith('pytroll://oper/polar/direct_readout/bla heartbeat '))
        self.assertTrue(msg.endswith(' v1.01 application/json {"next_pass_time": "unknown", "addr": "hej"}'))
        self.assertEquals(self.pub.send.call_count, 1)

        
class TestPublisher(unittest.TestCase):

    def setUp(self):
        self.socket = MagicMock(name="socket")
        self.socket.bind = MagicMock(name="bind")
        self.context = MagicMock(name="context")
        self.context.socket = MagicMock(name="socket_fun",
                                        return_value=self.socket)

    def test_publisher(self):
        """Test the publisher
        """

        port = int(random.uniform(1024, 65536))
        
        pub = server.Publisher(self.context, port)
        self.assertEquals(self.context.socket.call_count, 1)
        self.socket.bind.assert_called_once_with("tcp://*:" + str(port))
        pub.send("hej")
        self.socket.send.assert_called_once_with("hej")
        pub.stop()
        self.assertEquals(self.socket.close.call_count, 1)

    def test_race_conditions(self):
        """Test the race conditions in the send function
        """

        def wait(*args, **kwargs):
            time.sleep(0.1)

        context = MagicMock()
        context.socket.return_value.send = MagicMock(side_effect=wait)
        port = int(random.uniform(1024, 65536))
        pub = server.Publisher(context, port)
        thr = Thread(target=pub.send, args=["send 1"])
        thr.start()
        time.sleep(0.05)
        context.socket.return_value.send.side_effect = None
        pub.send("send 2")
        thr.join()
        self.assertEquals(pub._socket.send.call_args_list[0][0][0], "send 1")
        self.assertEquals(pub._socket.send.call_args_list[1][0][0], "send 2")
        
class TestHolder(unittest.TestCase):

    def setUp(self):
        self.pub = MagicMock()
        self.origin = MagicMock()
        self.holder = server.Holder(self.pub, self.origin)

    def test_have(self):
        """Test Holder.have
        """
        self.holder.have(1, 2, 3, 4)
        server.Message.assert_called_with(server.subject,
                                          'have',
                                          {"satellite": 1,
                                           "timecode": 2,
                                           "elevation": 3,
                                           "quality": 4,
                                           "origin": self.origin})
        self.pub.send.assert_called_with(server.Message.return_value.encode.return_value)

    # FIXME: test race conditions on add/get_data
    @patch.object(server.Holder, 'have')
    def test_add_delete(self, have):
        """Test Holder.add and Holder.delete
        """
        args = (1, 2, 3, 4, 5)

        self.holder.add(*args)
        have.assert_called_once_with(*args[:-1])

        self.assertEquals(args[-1], self.holder.get_data(*args[:2]))

        self.assertTrue(args[1] in self.holder.get_sat(args[0]))

        self.holder.delete(*args[:2])

        self.assertRaises(KeyError, self.holder.get, *args[:2])

    
        

# CADU
# HRPT
# FileWatcher
# MirrorWatcher
# Cleaner

class TestRequestManager(unittest.TestCase):

    def setUp(self):
        self.holder = MagicMock()
        self.context = MagicMock()
        self.port = MagicMock()
        self.station = MagicMock()
        
        self.reqman = server.RequestManager(self.holder,
                                            self.context,
                                            self.port,
                                            self.station)
        

    def test_start_stop(self):
        """Test start and stop of ReqMan
        """
        self.reqman.start()
        self.assertTrue(self.reqman._loop)
        self.reqman.stop()
        self.assertFalse(self.reqman._loop)

    def test_pong(self):
        """Test response to ping
        """
        self.reqman.pong()
        server.Message.assert_called_with(server.subject, "pong",
                                               {"station": self.station})

    def test_notice(self):
        """Test response to notice
        """
        self.reqman.notice(MagicMock())
        server.Message.assert_called_with(server.subject, "ack")
                       
    def test_unknown(self):
        """Test unknown
        """
        self.reqman.unknown(MagicMock())
        server.Message.assert_called_with(server.subject, "unknown")

    @patch.object(server, "strp_isoformat")
    def test_scanline(self, strpfun):
        """Test scanline response.
        """
        resp = self.reqman.scanline(MagicMock())
        server.Message.assert_called_with(server.subject, "scanline",
                                          self.holder.get_data.return_value,
                                          binary=True)
        self.holder.get_data.side_effect = KeyError("boom")
        resp = self.reqman.scanline(MagicMock())
        server.Message.assert_called_with(server.subject, "missing")
        self.holder.get_data.side_effect = None
        
    def test_send(self):
        """Test sending response.
        """
        msg = MagicMock()
        self.reqman.send(msg)
        self.context.socket.return_value.send.assert_called_once_with(str(msg))

    @patch.object(server, "strp_isoformat")
    def test_run(self, strp_fun):
        """Test running the ReqMan
        """
        socket = self.context.socket.return_value
        socks = ((socket, server.POLLIN),)


        cnt = [0]
        
        def side_effect(timeout):
            del timeout
            time.sleep(0.1)
            cnt[0] += 1
            return socks

        ping = MagicMock()
        ping.type = "ping"

        req = MagicMock()
        req.type = "request"
        req.data = {"type": "scanline",
                    "satellite": MagicMock(),
                    "utctime": MagicMock()}

        notice = MagicMock()
        notice.type = "notice"
        notice.data = {"type": "scanline"}

        unknown = MagicMock()

        msgs = [ping, req, notice, unknown]
        
        def msg(*args, **kwargs):
            if "rawstr" in kwargs:
                return msgs[cnt[0] % len(msgs)]
            else:
                return MagicMock()

        server.Message.side_effect = msg
        
        self.reqman.pong = MagicMock()
        self.reqman.notice = MagicMock()
        self.reqman.scanline = MagicMock()
        self.reqman.unknown = MagicMock()
        sys.modules["zmq"].Poller.return_value.poll.side_effect = side_effect
        self.reqman.start()
        time.sleep(0.4)
        self.reqman.stop()
        self.reqman.join()
        self.reqman.pong.assert_called_once_with()
        self.reqman.notice.assert_called_once_with(notice)
        self.reqman.scanline.assert_called_once_with(req)
        self.reqman.unknown.assert_called_once_with(unknown)
        sys.modules["zmq"].Poller.return_value.side_effect = None
        server.Message.side_effect = None

class TestServe(unittest.TestCase):

    @patch.object(time, 'sleep')
    @patch.object(server, 'RequestManager')
    @patch.object(server, 'MirrorWatcher')
    @patch.object(server, 'FileWatcher')
    @patch.object(server, 'Cleaner')
    @patch.object(server, 'Holder')
    @patch.object(server, 'Heart')
    @patch.object(server, 'Publisher')
    def test_serve_mirror(self, Publisher, Heart, Holder, Cleaner, FileWatcher,
                          MirrorWatcher, RequestManager, sleep):
        """Test serving from a mirror
        """
        server.ConfigParser.return_value.read.reset_mock()
        server.Context.return_value.term.reset_mock()
        server.time.sleep = MagicMock(side_effect=KeyboardInterrupt())
        mirrorcfg = ["this_computer", "neverwhere", "360 180 100",
                     "/some/where/", "amore_mio",
                     "/under/the/rainbow/", "bluebird"]
        server.ConfigParser.return_value.get = MagicMock(side_effect=mirrorcfg)
        mirrorcfg = [666, 667, 668, 669]
        server.ConfigParser.return_value.getint = MagicMock(side_effect=mirrorcfg)
        server.serve("test_config.cfg")

        # Config reading
        
        server.ConfigParser.return_value.read.assert_called_once_with("test_config.cfg")

        # publisher
        Publisher.assert_called_once_with(server.Context.return_value, 666)
        
        # heart
        Heart.assert_called_once_with(Publisher.return_value,
                                      "amore_mio:666",
                                      30)

        # holder
        Holder.assert_called_once_with(Publisher.return_value,
                                       "amore_mio:666")

        # cleaner
        Cleaner.assert_called_once_with(Holder.return_value, 1)

        # watcher
        self.assertEquals(FileWatcher.call_count, 0)
        
        MirrorWatcher.assert_called_once_with(Holder.return_value,
                                              server.Context.return_value,
                                              "bluebird", 667, 668)
        # request manager

        RequestManager.assert_called_once_with(Holder.return_value,
                                               server.Context.return_value,
                                               669, "neverwhere")
        
        # closing apps...
        for app in [RequestManager.return_value,
                    MirrorWatcher.return_value,
                    Cleaner.return_value,
                    Heart.return_value,
                    Publisher.return_value]:
            self.assertEquals(app.stop.call_count, 1)
        self.assertEquals(server.Context.return_value.term.call_count, 1)

    @patch.object(time, 'sleep')
    @patch.object(server, 'RequestManager')
    @patch.object(server, 'MirrorWatcher')
    @patch.object(server, 'FileWatcher')
    @patch.object(server, 'Cleaner')
    @patch.object(server, 'Holder')
    @patch.object(server, 'Heart')
    @patch.object(server, 'Publisher')
    def test_serve_file(self, Publisher, Heart, Holder, Cleaner, FileWatcher,
                          MirrorWatcher, RequestManager, sleep):
        """Test serving from a file
        """
        server.ConfigParser.return_value.read.reset_mock()
        server.Context.return_value.term.reset_mock()
        server.time.sleep = MagicMock(side_effect=KeyboardInterrupt())
        server.NoOptionError = Exception
        filecfg = ["this_computer", "neverwhere", "360 180 100",
                     "/some/where/", "amore_mio",
                     "/tmp", "*.hmf", server.NoOptionError("boom")]
        server.ConfigParser.return_value.get = MagicMock(side_effect=filecfg)
        filecfg = [666, 667, 668, 669]
        server.ConfigParser.return_value.getint = MagicMock(side_effect=filecfg)
        server.serve("test_config.cfg")

        # Config reading
        server.ConfigParser.return_value.read.assert_called_once_with("test_config.cfg")

        # publisher
        Publisher.assert_called_once_with(server.Context.return_value, 666)
        
        # heart
        Heart.assert_called_once_with(Publisher.return_value,
                                      "amore_mio:666",
                                      30)

        # holder
        Holder.assert_called_once_with(Publisher.return_value,
                                       "amore_mio:666")

        # cleaner
        Cleaner.assert_called_once_with(Holder.return_value, 1)

        # watcher
        self.assertEquals(MirrorWatcher.call_count, 0)
        
        FileWatcher.assert_called_once_with(Holder.return_value,
                                            "/tmp/*.hmf")
        # request manager

        RequestManager.assert_called_once_with(Holder.return_value,
                                               server.Context.return_value,
                                               667, "neverwhere")
        
        # closing apps...
        for app in [RequestManager.return_value,
                    FileWatcher.return_value,
                    Cleaner.return_value,
                    Heart.return_value,
                    Publisher.return_value]:
            self.assertEquals(app.stop.call_count, 1)
        self.assertEquals(server.Context.return_value.term.call_count, 1)
        server.NoOptionError = MagicMock()
        
def suite():
    """The suite for test_server
    """
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestHeart))
    mysuite.addTest(loader.loadTestsFromTestCase(TestPublisher))
    mysuite.addTest(loader.loadTestsFromTestCase(TestHolder))
    mysuite.addTest(loader.loadTestsFromTestCase(TestRequestManager))
    mysuite.addTest(loader.loadTestsFromTestCase(TestServe))
    
    return mysuite
