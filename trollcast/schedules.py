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

"""Read schedule files.
"""

from datetime import datetime, timedelta

formats = ["scisys"]
satellites = ["NOAA 15", "NOAA 16", "NOAA 18", "NOAA 19"]


def scisys(filename):
    """Read a scisys schedule
    """
    with open(filename) as fp_:
        while True:
            line = fp_.readline()
            try:
                if line[0] in ["\n", " ", "!", "#"]:
                    continue
            except IndexError:
                break
            elts = line[16:].split()
            sat = line[:16].strip()
            rec = elts[8] == "Y"
            if rec and sat in satellites:
                rise, fall = "".join(elts[1:3]), "".join(elts[3:5])
                rise = datetime.strptime(rise, "%Y%m%d%H%M%S")
                fall = datetime.strptime(fall, "%Y%m%d%H%M%S")
                yield (rise, sat, fall)


def kongsberg_metno(filename):
    """Read a kongsberg schedule
    """
    with open(filename) as fp_:
        while True:
            line = fp_.readline()
            if line.startswith(" No."):
                continue

            elts = line.split()
            try:
                sat = elts[2].replace("_", " ")
            except IndexError:
                break
            rec = elts[13].startswith("Schedule")
            if rec and sat in satellites:
                rise = " ".join((elts[1], elts[5]))
                rise = datetime.strptime(rise, "%Y%m%d %H:%M:%S")
                mins, secs = elts[8].split(":")
                fall = rise + timedelta(minutes=int(mins), seconds=int(secs))
                yield (rise, sat, fall)
