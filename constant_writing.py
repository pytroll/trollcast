#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2012, 2013, 2014 SMHI

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

"""Continuous writing to test inotify
"""


import time
import os
import sys
from datetime import datetime, timedelta
from random import choice
import numpy as np
from hrpt_reader2 import read_file, timecode

try:
    os.mkdir("/tmp/trolltest")
except OSError:
    pass

filename = sys.argv[1]
arr = read_file(filename, swap=True)

SATELLITES = {7: "NOAA 15",
              3: "NOAA 16",
              13: "NOAA 18",
              15: "NOAA 19"}

satellites = [7, 3, 13, 15]

def mk_tc(utctime):
    tc_array = np.zeros((4,), dtype="<u2")
    epoch = datetime(utctime.year, 1, 1)

    td = utctime - epoch
    tc_array[0] = np.uint16((td.days + 1) << 1)
    msecs = td.seconds * 1000 + td.microseconds / 1000
    tc_array[1] = np.uint16(msecs >> 20)
    tc_array[2] = np.uint16((msecs & (1023 << 10)) >> 10)
    tc_array[3] = np.uint16(msecs & 1023)

    return tc_array

def mk_id(old_id, satid):
    new_id = old_id & 0b1110000111
    new_id |= satid << 3
    return new_id

def satid(gid):
    return (gid >> 3) & 0b1111

now = datetime.now()
print(now - timecode(mk_tc(now)))

delay = 0.166

while True:
    start_time = datetime.now()
    sat = choice(satellites)
    satname = "_".join(SATELLITES[sat].split())
    new_filename = "_".join(
        (start_time.strftime("%Y%m%d%H%M%S"), satname)) + ".temp"
    new_filename = os.path.join("/tmp/trolltest", new_filename)
    print("choosing", SATELLITES[sat])
    print("filename", new_filename)
    print("start_time", start_time)
    with open(new_filename, "wb") as fpw:
        for i, line in enumerate(arr):
            line["id"]["id"] = mk_id(line["id"]["id"], sat)
            new_timecode = mk_tc(start_time + i * timedelta(seconds=delay))
            line["timecode"][0] = new_timecode[0]
            line["timecode"][1] = new_timecode[1]
            line["timecode"][2] = new_timecode[2]
            line["timecode"][3] = new_timecode[3]
            line.tofile(fpw)
            time.sleep(delay)
    time.sleep(3)
    os.remove(new_filename)
