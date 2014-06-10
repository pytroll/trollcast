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

"""Test the trollcast client
"""

import unittest
import trollcast.client
from trollcast.client import Client, Requester
from datetime import datetime
import time
from threading import Thread
from numpy import arange

from mock import mock_open, patch, MagicMock



m = mock_open()

class TestClient(unittest.TestCase):
    @patch("trollcast.client.Requester")
    @patch("trollcast.client.create_requesters", MagicMock())
    @patch("trollcast.client.create_timers", MagicMock())
    @patch("trollcast.client.create_subscriber", MagicMock())
    @patch('trollcast.client.open', m, create=True)
    def test_get_all(self, requester_class):
        client = Client()
        thr = Thread(target=client.get_all, args=(["NOAA 17"],))
        thr.start()
        for elev in arange(80.01, 0.0, -0.5):
            queue = client._queues[0]
            queue.put(("NOAA 17", datetime.utcnow(), [("localhost", elev, 100)]))
            time.sleep(0.01)
        time.sleep(4)
        self.assertTrue(len(m.mock_calls))
        client.stop()
        thr.join()


def suite():
    """The suite for test_client
    """
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestClient))

    return mysuite

if __name__ == '__main__':
    unittest.main()
