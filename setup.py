#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2012, 2014 Martin Raspaud

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

"""Setup Trollcast
"""

from setuptools import setup
import sys
import imp

version = imp.load_source('trollcast.version', 'trollcast/version.py')


requirements = ['pyzmq', 'pyorbital', 'posttroll', 'numpy', 'pyinotify']
if sys.version_info < (2, 7):
    requirements.append('argparse')


setup(name="trollcast",
      version=version.__version__,
      description='Weather satellite data exchange tool',
      author='The pytroll team',
      author_email='martin.raspaud@smhi.se',
      packages=['trollcast'],
      scripts = ['bin/trollcast_client',
                 'bin/trollcast_server'],
      zip_safe=False,
      license="GPLv3",
      install_requires=requirements,
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
          'Programming Language :: Python',
          'Operating System :: OS Independent',
          'Intended Audience :: Science/Research',
          'Topic :: Scientific/Engineering',
          'Topic :: Communications'
          ],
      #extras_require={ 'daemon': ['python-daemon'],}
      )
