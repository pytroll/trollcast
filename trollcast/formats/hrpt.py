#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2013 Martin Raspaud

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

"""Implementation of the hrpt format.
"""

import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

satellites = {13: "NOAA 18"}

class HRPTReader(object):

    line_per_sec = 6

    line_size = 11090 * 2

    sync = np.array([ 994, 1011, 437, 701, 644, 277, 452, 467, 833, 224, 694,
                      990, 220, 409, 1010, 403, 654, 105, 62, 867, 75, 149,
                      320, 725, 668, 581, 866, 109, 166, 941, 1022, 59, 989,
                      182, 461, 197, 751, 359, 704, 66, 387, 238, 850, 746,
                      473, 573, 282, 6, 212, 169, 623, 761, 979, 338, 249, 448,
                      331, 911, 853, 536, 323, 703, 712, 370, 30, 900, 527,
                      977, 286, 158, 26, 796, 705, 100, 432, 515, 633, 77, 65,
                      489, 186, 101, 406, 560, 148, 358, 742, 113, 878, 453,
                      501, 882, 525, 925, 377, 324, 589, 594, 496, 972],
                      dtype=np.uint16)


    sync_start = np.array([644, 367, 860, 413, 527, 149], dtype=np.uint16)

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


    def read_line(self, line):
        if len(line) != self.line_size:
            raise ValueError("packet length is incorrect")
        array = np.fromstring(line, dtype=self.dtype)[0]
        if np.all(abs(self.sync_start - 
                      array["frame_sync"]) > 1):
            array = array.newbyteorder()

        satellite = satellites[(array["id"]["id"] >> 3) & 15]

        # FIXME: this is bad!!!! Should not get the year from the filename
        year = 2012
        utctime = datetime(year, 1, 1) + timecode(array["timecode"])

        # Check that we receive good data
        if not (np.all(array['aux_sync'] == self.sync) and
                np.all(array['frame_sync'] == self.sync_start)):
            logger.info("Garbage line: " + str(utctime))
            line = self._file.read(self.line_size)
            return

        return utctime, satellite, line


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
    # FIXME : should return current year !
    return timedelta(days=int(day/2 - 1), milliseconds=int(msecs))
