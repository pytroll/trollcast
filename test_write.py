#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2012 SMHI

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
from __future__ import with_statement 

import time
import os
import sys
from datetime import datetime

FILENAME = "/home/a001673/data/20151102041257_NPP.cadu"

try:
    os.mkdir("/tmp/cadu")
except OSError:
    pass

try:
    start_time = datetime.strptime(sys.argv[1], "%Y%m%d%H%M%S")
    if start_time > datetime.utcnow():
        time.sleep((start_time - datetime.utcnow()).seconds)
except IndexError:
    pass

try:
    with open(FILENAME, "rb") as fpr:
        with open("/tmp/cadu/20151102041257_NPP.temp", "wb") as fpw:
            truc = fpr.read(3000)
            cnt = 0
            while truc:
                cnt += 1
                fpw.write(truc)
                truc = fpr.read(1024 * 166)
                time.sleep(1.0/6)
finally:
    print 'stopped at', datetime.utcnow()
