.. Trollcast documentation master file, created by
   sphinx-quickstart on Wed Nov 14 12:57:53 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Trollcast's documentation!
=====================================

To the `source code page <http://github.com/mraspaud/trollcast>`_.

Trollcast is a tool to exchange polar weather satellite data. It aims at
providing near real time data transfer between peers, and should be adaptable
to any type of data that is scan-based. At the moments it works on hrpt minor
frame data (both big and little endian).

The protocol it uses is loosely based on bittorrent.

.. warning::
  This is experimental software, use it at your own risk!

Installing trollcast
--------------------

Download trollcast from the `source code page
<http://github.com/mraspaud/trollcast>`_ and run::

  python setup.py install

Setting up trollcast
--------------------

A trollcast config file describes the different parameters one needs for
running both the client and the server.

.. code-block:: ini
  
  [local_reception]
  localhost=nimbus
  remotehosts=safe
  data=hrpt
  data_dir=/data/hrpt
  file_pattern=*.temp
  max_connections=2
  station=norrköping
  coordinates=16.148649 58.581844 0.02
  tle_dir=/var/opt/2met/data/polar/orbitalelements/
  
  [safe]
  hostname=172.29.0.236
  pubport=9333
  reqport=9332
  
  [nimbus]
  hostname=172.22.8.16
  pubport=9333
  reqport=9332

The `local_reception` section
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

 - `localhost` defines the name of the host the process is going to run on
   locally. This name will be user further down in the configuration file as a
   section which will hold information about the host. More on this later.
 - `remotehosts` is the list of remote hosts to communicate with.
 - `data` give the type of data to be exchange. Only *hrpt* is available at the
   moment.
 - `data_dir` is the place where streaming data from the reception station is
   written. 
 - `file_pattern` is the fnmatch pattern to use to detect the file that the
   reception station writes to. Trollcast will watch this file to stream the
   data to the network in real time.
 - `max_connections` tells how many times the data can be sent. This is usefull
   for avoiding too many clients retrieving the data from the same server,
   putting unnecessary load on it. Instead, clients will spread the data among
   each other, creating a more distributed load.
 - `station`: name of the station
 - `coordinates`: coordinates of the station. Used for the computation of
   satellite elevation.
 - `tle_dir`: directory holding the latest TLE data. Used for the computation
   of satellite elevation.

The host sections
~~~~~~~~~~~~~~~~~
   
 - `hostname` is the hostname or the ip address of the host.
 - `pubport` on which publishing of messages will occur.
 - `reqport` on which request and transfer of data will occur.

Modes of operation
------------------

Server mode, giving out data to the world
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The server mode is used to serve data to remote hosts.

It is started with::
   trollcast_server my_config_file.cfg

This will start a server that watches a given file, as specified in the
configuration file. Add a ``-v`` if you want debugging info.

.. note::

   In the eventuality that you want to start a sever in gateway mode, that is
   acting as a gateway to another server, add
   ``mirror=name_of_the_primary_server`` in your configuration file.

Client mode, retrieving data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The client mode retrieves data. 

Here is the usage of the client::

 usage: client.py [-h] [-t TIMES TIMES] [-o OUTPUT] -f CONFIG_FILE
                  satellite [satellite ...]

 positional arguments:
   satellite             eg. noaa_18

 optional arguments:
   -h, --help            show this help message and exit
   -t TIMES TIMES, --times TIMES TIMES
                         Start and end times, <YYYYMMDDHHMMSS>
   -o OUTPUT, --output OUTPUT
                         Output file (used only in conjuction with -t)
   -f CONFIG_FILE, --config_file CONFIG_FILE
                         eg. sattorrent_local.cfg
   -v, --verbose

There are two ways of running the client:
 - The first way is to retrieve a given time interval of data. For example, to
   retrieve data from NOAA 18 for the 14th of November 2012, between 14:02:23
   and 14:15:00, the client has to be called with::

     trollcast_client -t 20121114140223 20121114141500 -o noaa18_20121114140223.hmf -f config_file.cfg noaa_18
 - The second way is to retrieve all the data possible data and dump it to
   files::

     trollcast_client -f config_file.cfg noaa_15 noaa_16 noaa_18 noaa_19

   In this case, only new data will be retrieved though, contrarily to the time
   interval retrieval where old data will be retrieved too if necessary.

Contents:

.. toctree::
   :maxdepth: 2

API
===

Client
------

.. automodule:: trollcast.client
   :members:
   :undoc-members:

Server
------

.. automodule:: trollcast.server
   :members:
   :undoc-members:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

