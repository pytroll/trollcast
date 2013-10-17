#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2013 Martin Raspaud, Janne Kotro, Timo Ryyppö

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>
#   Janne Kotro <janne.kotro@fmi.fi>
#   Timo Ryyppö <timo.ryyppo@fmi.fi>

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

"""Reader for the cadu format.

'Joint Polar Satellite System (JPSS) Common Data Format Control Book – External
(CDFCB-X) Volume VII – Part 1 JPSS Downlink Data Formats'


"""

from datetime import datetime, timedelta
import numpy as np
from trollcast.formats.reedsolomon import *

import logging
logger = logging.getLogger(__name__)

table = np.array([0xff, 0x48, 0x0e, 0xc0, 0x9a, 0x0d, 0x70, 0xbc, 0x8e, 0x2c,
                  0x93, 0xad, 0xa7, 0xb7, 0x46, 0xce, 0x5a, 0x97, 0x7d, 0xcc,
                  0x32, 0xa2, 0xbf, 0x3e, 0x0a, 0x10, 0xf1, 0x88, 0x94, 0xcd,
                  0xea, 0xb1, 0xfe, 0x90, 0x1d, 0x81, 0x34, 0x1a, 0xe1, 0x79,
                  0x1c, 0x59, 0x27, 0x5b, 0x4f, 0x6e, 0x8d, 0x9c, 0xb5, 0x2e,
                  0xfb, 0x98, 0x65, 0x45, 0x7e, 0x7c, 0x14, 0x21, 0xe3, 0x11,
                  0x29, 0x9b, 0xd5, 0x63, 0xfd, 0x20, 0x3b, 0x02, 0x68, 0x35,
                  0xc2, 0xf2, 0x38, 0xb2, 0x4e, 0xb6, 0x9e, 0xdd, 0x1b, 0x39,
                  0x6a, 0x5d, 0xf7, 0x30, 0xca, 0x8a, 0xfc, 0xf8, 0x28, 0x43,
                  0xc6, 0x22, 0x53, 0x37, 0xaa, 0xc7, 0xfa, 0x40, 0x76, 0x04,
                  0xd0, 0x6b, 0x85, 0xe4, 0x71, 0x64, 0x9d, 0x6d, 0x3d, 0xba,
                  0x36, 0x72, 0xd4, 0xbb, 0xee, 0x61, 0x95, 0x15, 0xf9, 0xf0,
                  0x50, 0x87, 0x8c, 0x44, 0xa6, 0x6f, 0x55, 0x8f, 0xf4, 0x80,
                  0xec, 0x09, 0xa0, 0xd7, 0x0b, 0xc8, 0xe2, 0xc9, 0x3a, 0xda,
                  0x7b, 0x74, 0x6c, 0xe5, 0xa9, 0x77, 0xdc, 0xc3, 0x2a, 0x2b,
                  0xf3, 0xe0, 0xa1, 0x0f, 0x18, 0x89, 0x4c, 0xde, 0xab, 0x1f,
                  0xe9, 0x01, 0xd8, 0x13, 0x41, 0xae, 0x17, 0x91, 0xc5, 0x92,
                  0x75, 0xb4, 0xf6, 0xe8, 0xd9, 0xcb, 0x52, 0xef, 0xb9, 0x86,
                  0x54, 0x57, 0xe7, 0xc1, 0x42, 0x1e, 0x31, 0x12, 0x99, 0xbd,
                  0x56, 0x3f, 0xd2, 0x03, 0xb0, 0x26, 0x83, 0x5c, 0x2f, 0x23,
                  0x8b, 0x24, 0xeb, 0x69, 0xed, 0xd1, 0xb3, 0x96, 0xa5, 0xdf,
                  0x73, 0x0c, 0xa8, 0xaf, 0xcf, 0x82, 0x84, 0x3c, 0x62, 0x25,
                  0x33, 0x7a, 0xac, 0x7f, 0xa4, 0x07, 0x60, 0x4d, 0x06, 0xb8,
                  0x5e, 0x47, 0x16, 0x49, 0xd6, 0xd3, 0xdb, 0xa3, 0x67, 0x2d,
                  0x4b, 0xbe, 0xe6, 0x19, 0x51, 0x5f, 0x9f, 0x05, 0x08, 0x78,
                  0xc4, 0x4a, 0x66, 0xf5, 0x58], dtype=np.uint8)

def decode_str(arr):
    """PN decode the data.
    """
    decoder = np.tile(table, np.ceil(len(arr) / 255.0))
    return (np.fromstring(arr, np.uint8)^decoder[:len(arr)]).tostring()

vcdu_pri_hdr_type = np.dtype([('frame_sync', '>u1', (4, )),
                              ('version', '>u2'),
                              ('vcdu count', '>u1', (3, )),
                              ('replay', '>u1')])

m_pdu_hdr_type = np.dtype([('hdr_ccsds_offset', '>u2')])

ccsds_hdr_type = np.dtype([('ccsds_version', '>u2'),
                           ('sequence', '>u2'),
                           ('packet_length', '>u2'),
                           ('days', '>u2'),
                           ('milliseconds', '>u4'),
                           ('microseconds', '>u2'),
                           ('seq_count', '>u1'),
                           ('spare', '>u1')])

ccsds_pri_hdr_type = np.dtype([('ccsds_version', '>u2'),
                               ('sequence', '>u2'),
                               ('packet_length', '>u2')])
ccsds_sec_hdr_type = np.dtype([('days', '>u2'),
                               ('milliseconds', '>u4'),
                               ('microseconds', '>u2')])

def get_info(buff):
    """print the detail of the packet in buffer.
    """

    arr1 = np.fromstring(buff, dtype=vcdu_pri_hdr_type, count=1)

    print "sync", arr1["frame_sync"]
    print "version number", arr1["version"] >> 14
    print "spacecraft id", (arr1["version"] >> 6) & 255
    print "vcid", (arr1["version"]) & 63
    
    #arr2 = np.fromstring(buff[10:], dtype=m_pdu_hdr_type, count=1)
    #arr3 = np.fromstring(buff[12 + offset:], dtype=ccsds_hdr_type, count=1)

    

def timecode(arr):
    #print bin(arr['days']), bin(arr['milliseconds']), bin(arr['microseconds'])
    #print arr['days'], arr['milliseconds'], arr['microseconds']
    return (timedelta(days=arr['days'] * 1.0,
                      milliseconds=arr['milliseconds']
                      + arr['microseconds'] / 1000.0)
            + datetime(1958, 1, 1))


spacecrafts = {0x2A: "TERRA",
               0x9A: "AQUA",
               0x9D: "SUOMI NPP"}

class CADUReader(object):

    line_size = 1024

    def __init__(self):
        self.timestamp = {}
        self.satellite = None
        self.decode = False
        self.offset = 4
        self.last_received = None
        self.data = ""
        self.vcids = {}

    def reset(self):
        """Reset the reader.
        """
        self.timestamp = {}
        self.satellite = None
        self.decode = False
        self.offset = 4
        self.last_received = None
        self.data = ""

    def read_line(self, raw_packet, decode=None, decode_offset=None):
        # TODO: apriori knowledge of satellite and encoding should be passed in
        # here!
        if len(raw_packet) != 1024:
            raise ValueError("packet is not 1024 bytes long")

        if ((self.last_received is None) or
            ((datetime.utcnow() - self.last_received) > timedelta(seconds=5))):
            logger.info("resetting")
            self.reset()
            
        if decode is not None:
            self.decode = decode
        if decode_offset is not None:
            self.offset = decode_offset
        if self.decode:
            packet = (raw_packet[:self.offset] +
                      decode_str(raw_packet[self.offset:896]) +
                      raw_packet[896:])
        else:
            packet = raw_packet

        arr = np.fromstring(packet, dtype=vcdu_pri_hdr_type, count=1)[0]
        vcount = (arr["vcdu count"][0] * 256 ** 2 +
                  arr["vcdu count"][1] * 256 +
                  arr["vcdu count"][2])
        vcid = arr["version"] & 63
        #logger.debug(str(vcid) + " " + str(vcount))
        try:
            # TODO: check also version number and VCID to be thorough
            satellite = spacecrafts[(arr["version"] >> 6) & 255]
        except KeyError:
            if not self.decode: # Aqua, NPP
                return self.read_line(raw_packet, True)
            else:
                logger.info("spurious satellite number, skipping: " + str((arr["version"] >> 6) & 255))
            return

        self.vcids.setdefault(vcid, []).append(vcount)
        
        arr = np.fromstring(packet[10:], dtype=m_pdu_hdr_type, count=1)[0]
        offset = (arr["hdr_ccsds_offset"] & (2**11 - 1))
        if offset != 2047:
            try:
                arr = np.fromstring(packet[12+offset:], dtype=ccsds_pri_hdr_type,
                                    count=1)[0]
                offset += 6
            except ValueError:
                if not self.decode and satellite=='TERRA': # Try Terra case
                    return self.read_line(raw_packet, True, 10)
                else:
                    logger.info("Wrong offset: " + str(offset))
                    return

            packet_length = arr["packet_length"]
            sec_hdr = (arr["ccsds_version"] >> 11) & 1
            if sec_hdr != 0:
                arr = np.fromstring(packet[12+offset:], dtype=ccsds_sec_hdr_type,
                                    count=1)[0]
                new_timestamp = timecode(arr)
                if new_timestamp is not None:
                    if abs(datetime.utcnow() - new_timestamp) > timedelta(days=360):
                        return
                    self.satellite = satellite
                    self.last_received = datetime.utcnow()
                    self.timestamp[vcid] = new_timestamp
                    return (vcid, vcount, new_timestamp), satellite, packet
                
        return (vcid, vcount, self.timestamp[vcid]), satellite, packet

def dict_ccsds_pri(arr):
    apid =  (arr['ccsds_version']) & 0x7ff
    sec_hdr = (arr["ccsds_version"] >> 11) & 1
    packet_type = (arr["ccsds_version"] >> 12) & 1
    version = (arr["ccsds_version"] >> 13)
    packet_count = arr["sequence"] & 0x3fff
    sequence = arr["sequence"] >> 14
    packet_length = arr["packet_length"]
    return {"version":  version,
            "type": packet_type,
            "secondary_header": sec_hdr,
            "apid": apid,
            "packet_count": packet_count,
            "sequence": sequence,
            "packet_length": packet_length}

def repr_ccsds_pri(arr):
    apid =  (arr['ccsds_version']) & 0x7ff
    sec_hdr = (arr["ccsds_version"] >> 11) & 1
    packet_type = (arr["ccsds_version"] >> 12) & 1
    version = (arr["ccsds_version"] >> 13)
    return ("version: " + str(version) + "\n" +
            "type: " + str(packet_type) + "\n" +
            "secondary header: " + str(sec_hdr) + "\n" +
            "apid: " + str(apid))

vcids = {"TERRA":
         {0x2A: "MODIS",
          0x29: "MISR",
          0x3F: "fill"},
         "AQUA":
         {0x1E: "MODIS"},
         "SUOMI NPP":
         {0xFF: "fill cadu",
          0x3F: "fill vcdu"}
         }

frames = {}
delcnt = 0
pcnt = 0

def read_frame(data):
    global frames
    global delcnt
    global pcnt
    offset = 4
    import struct
    #new_data = bytearray(decode_str(data))
    new_data = bytearray(data)

    # VCDU primary header

    vcdu_pri_hdr = np.dtype([('version', '>u2'),
                             ('vcdu count', '>u1', (3, )),
                             ('replay', '>u1')])
    arr1 = np.fromstring(str(new_data[offset:]), dtype=vcdu_pri_hdr, count=1)
    version_number = arr1["version"][0] >> 14
    spacecraft_id = ((arr1["version"][0] >> 6) & 255)
    if version_number != 1 or spacecraft_id not in spacecrafts:
        #print "decoding"
        new_data = bytearray(decode_str(data))
        
    arr1 = np.fromstring(str(new_data[offset:]), dtype=vcdu_pri_hdr, count=1)
    version_number = arr1["version"][0] >> 14
    spacecraft_id = ((arr1["version"][0] >> 6) & 255)
    vcid = arr1["version"][0] & 0x3f
    if version_number != 1 or spacecraft_id not in spacecrafts:
        print "hopeless, giving up"
    try:
        spacecraft_name = spacecrafts[spacecraft_id]
    except KeyError:
        spacecraft_name = "unknown"
        
    try:
        instrument = vcids[spacecraft_name][vcid]
    except KeyError:
        instrument = "unknown"

    truc = bytearray("\0\0\0\0")
    truc[1] = arr1["vcdu count"][0][0]
    truc[2] = arr1["vcdu count"][0][1]
    truc[3] = arr1["vcdu count"][0][2]
    offset += 6

    # VCDU insert zone

    # only for jpss
    if spacecraft_name.startswith("JPSS"):
        vcdu_insert_zone = np.dtype([('vcdu count', '>u1'),
                                     ('key', '>u2'),
                                     ('spare', '>u1')])
        arr1 = np.fromstring(str(new_data[offset:]), dtype=vcdu_insert_zone, count=1)
        truc[0] = arr1["vcdu count"][0]
        offset += 4

    vcount = struct.unpack(">I", str(truc))[0]

    # MPDU primary header

    arr1 = np.fromstring(str(new_data[offset:]), dtype=m_pdu_hdr_type, count=1)
    if arr1["hdr_ccsds_offset"][0] >> 11 != 0:
        print "MPDU spare corrupted"

    fhp = arr1["hdr_ccsds_offset"][0] & 0x7ff
    if fhp == 2047:
        return
    if fhp == 2046:
        return
    if fhp <= 2045:
        offset += fhp

    offset += 2

    # CCSDS primary header


    # ccsds_hdr_type = np.dtype([('ccsds_version', '>u2'),
    #                        ('sequence', '>u2'),
    #                        ('packet_length', '>u2'),
    #                        ('days', '>u2'),
    #                        ('milliseconds', '>u4'),
    #                        ('microseconds', '>u2')])

    arr1 = np.fromstring(str(new_data[offset:]), dtype=ccsds_hdr_type, count=1)[0]
    #print arr1

    version_number = arr1['ccsds_version'] >> 13
    packet_type = (arr1['ccsds_version'] >> 12) & 1
    secondary_header = (arr1['ccsds_version'] >> 11) & 1
    apid =  (arr1['ccsds_version']) & 0x7ff
    if version_number != 0 and packet_type != 0:
        raise ValueError("Invalid version number and packet type.")
    #print "version number", arr1['ccsds_version'] >> 13
    #print "type", (arr1['ccsds_version'] >> 12) & 1
    #print "second header flag", (arr1['ccsds_version'] >> 11) & 1
    #print "apid", (arr1['ccsds_version']) & 0x7ff

    seq_flag = arr1['sequence'] >> 14
    seq_cnt = arr1['sequence'] & 0x3fff

    # CCSDS secondary header

    if secondary_header:
        timestamp = timecode(arr1)
        frames[vcid] = [timestamp, seq_flag]
        print vcid, timestamp
    else:
        timestamp = None
        if vcid in frames:
            frames[vcid][1] = seq_flag

    # if apid in frames:
    #     print apid, seq_cnt, "/", frames[apid]
    # else:
    #     print apid, seq_cnt, "/ ?"

    # if seq_flag == 0b00:
    #     print "continuation"
    # elif seq_flag == 0b01:
    #     print "first"
    # elif seq_flag == 0b10:
    #     print "last"
    # elif seq_flag == 0b11:
    #     print "standalone/fill"

    if vcid in frames:
        #print spacecraft_name, instrument, "seq", vcid, vcount, frames[vcid]
        pcnt += 1
    else:
        #print "throwing away", spacecraft_name, instrument, "seq", vcid, vcount
        delcnt += 1
    #print delcnt, pcnt

    

    #raw_input()
    #if not secondary_header:
    #    return



    
    
    #print np.uint32(arr1["vcdu count"][0][0]) << 16 + np.uint32(arr1["vcdu count"][0][1]) << 8 + arr1["vcdu count"][0][2]

    
    
    

def read_cadu(fname):
    print fname
    data = 1
    cr = CADUReader()
    with open(fname, "rb") as fp_:
        try:
            while data:
                data = fp_.read(1024)
                if ord(data[0]) != 0x1a and ord(data[1]) != 0xcf and ord(data[2]) != 0xfc and ord(data[3]) != 0x1d:
                    logger.warning("jammed sync")
                #read_frame(data)
                cr.read_line(data)
        except IndexError:
            logger.exception("Unexpected end of file")
    return cr
if __name__ == '__main__':
    
    import sys
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("cadu")
    filename = sys.argv[1]

    CR = read_cadu(filename)
    
