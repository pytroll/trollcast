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

"""Class for handling several timers at once.
"""

from threading import Thread, Event, Lock
import bisect
from datetime import datetime, timedelta
class _Timer(object):

    def __init__(self, deadline, function, args=[], kwargs={}):
        self.deadline = deadline
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def run(self):
        self.function(*self.args, **self.kwargs)

def index(a, x):
    'Locate the leftmost value exactly equal to *x* in a sorted list *a*'
    i = bisect.bisect_left(a, x)
    if i != len(a) and a[i] == x:
        return i
    raise ValueError

class MultiTimer(Thread):

    def __init__(self):
        Thread.__init__(self)
        self._timers = {}
        self._uids = {}
        self._times = []
        self._loop = True
        self._reset = Event()
        self._lock = Lock()
    def stop(self):
        self._loop = False

    def add(self, uid, interval, function, args=[], kwargs={}):
        deadline = datetime.now() + timedelta(seconds=interval)
        self._uids.setdefault(deadline, []).append(uid)
        self._timers[uid] = _Timer(deadline, function, args, kwargs)
        with self._lock:
            new_location = bisect.bisect_right(self._times, deadline)
            self._times.insert(new_location, deadline)

        if new_location == 0:
            self._reset.set()

    def cancel(self, uid):
        with self._lock:
            deadline = self._timers[uid].deadline
            del self._timers[uid]
            self._uids[deadline].remove(uid)
            if len(self._uids[deadline]) == 0:
                pos = index(self._times, deadline)
                self._times.remove(deadline)
                del self._uids[deadline]
                if pos == 0:
                    self._reset.set()


    def run(self):
        while self._loop:
            try:
                self._reset.clear()
                to_wait = self._times[0] - datetime.now()
                to_wait = (to_wait.days * 60 * 60 * 24 +
                           to_wait.seconds +
                           to_wait.microseconds / 1e6)
            except IndexError:
                to_wait = 1
                continue
            finally:
                self._reset.wait(to_wait)
            with self._lock:
                if not self._times:
                    continue
                if datetime.now() > self._times[0]:
                    for uid in self._uids[self._times[0]]:
                        self._timers[uid].run()
                        del self._timers[uid]
                        try:
                            self._uids[self._times[0]].remove(uid)
                        except ValueError:
                            pass
                        if not self._uids[self._times[0]]:
                            del self._uids[self._times[0]]
                    del self._times[0]
                self._reset.clear()


if __name__ == '__main__':

    def ecrit(msg):
        print msg

    from time import sleep
    m = MultiTimer()
    m.start()
    sleep(0.5)
    m.add("yo", 2, ecrit, ["time's up !"])
#    sleep(1)
#    m.cancel("yo")
    sleep(3)
    m.stop()
