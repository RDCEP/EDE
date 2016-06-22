from __future__ import print_function
import sys
import struct
from struct import unpack, pack
import numpy as np
import argparse


class RasterProcessingException(Exception):
    """Represents an exception that can occur during the processing of a raster file
    """

    def __init__(self, message):
        super(RasterProcessingException, self).__init__(message)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class Band(object):
    def __init__(self, is_offline, has_no_data_value, is_no_data_value, pixtype, nodata, data):
        self.is_offline = is_offline  # python bool
        self.has_no_data_value = has_no_data_value  # python bool
        self.is_no_data_value = is_no_data_value  # python bool
        self.pixtype = pixtype  # python integer
        self.nodata = nodata  # format fmts[pixtype] = corresponding python type according to struct doc
        self.data = data  # numpy array with dtype = dtypes[pixtype]


class Raster(object):
    def __init__(self, version, n_bands, scale_X, scale_Y, ip_X, ip_Y, skew_X, skew_Y, srid, width, height):
        self.version = version  # format H = python integer
        self.n_bands = n_bands  # H
        self.scale_X = scale_X  # d = python float
        self.scale_Y = scale_Y  # d
        self.ip_X = ip_X  # d
        self.ip_Y = ip_Y  # d
        self.skew_X = skew_X  # d
        self.skew_Y = skew_Y  # d
        self.srid = srid  # i = python integer
        self.width = width  # H
        self.height = height  # H
        self.bands = []

    def __str__(self):
        return ("Raster(version={},n_bands={},scale_X={},scale_Y={},ip_X={},ip_Y={},skew_X={},skew_Y={},"
                "srid={},width={},height={})".format(self.version, self.n_bands, self.scale_X, self.scale_Y,
                                                     self.ip_X, self.ip_Y, self.skew_X, self.skew_Y,
                                                     self.srid, self.width, self.height))

    def add_band(self, band):
        self.bands.append(band)

    def raster_to_wkb(self, endian):

        chunks = []

        try:
            buff = pack('B', endian)
        except struct.error as e:
            eprint(e)
            raise RasterProcessingException("Could not pack endian-ness!")

        chunks.append(buff)

        if endian == 0:
            endian = '>'
        elif endian == 1:
            endian = '<'

        try:
            buff = pack(endian + 'HHddddddiHH', self.version, self.n_bands, self.scale_X, self.scale_Y,
                        self.ip_X, self.ip_Y, self.skew_X, self.skew_Y, self.srid, self.width, self.height)
        except struct.error as e:
            eprint(e)
            raise RasterProcessingException("Could not pack raster header")

        chunks.append(buff)

        num_pixels = self.width * self.height

        for band in self.bands:

            fmts = ['?', 'B', 'B', 'b', 'B', 'h', 'H', 'i', 'I', 'f', 'd']
            fmt = fmts[band.pixtype]

            # Write out band header pixels
            bit1 = 0x80 if band.is_offline else 0
            bit2 = 0x40 if band.has_no_data_value else 0
            bit3 = 0x20 if band.is_no_data_value else 0

            bits = bit1 | bit2 | bit3 | (band.pixtype + 1)

            try:
                buff = pack(endian + 'b', bits)
            except struct.error as e:
                eprint(e)
                raise RasterProcessingException("Could not pack band header bits!")

            chunks.append(buff)

            # Write out nodata value
            try:
                buff = pack(endian + fmt, band.nodata)
            except struct.error as e:
                eprint(e)
                raise RasterProcessingException("Could not pack nodata value!")

            chunks.append(buff)

            # Write out actual data
            try:
                buff = pack('{}{}{}'.format(endian, num_pixels, fmt), *band.data.flatten())
            except struct.error as e:
                eprint(e)
                raise RasterProcessingException("Could not pack actual band data!")

            chunks.append(buff)

        return b''.join(chunks)

    def raster_to_hexwkb(self, endian):
        wkb = self.raster_to_wkb(endian)
        return wkb.encode("hex").upper()


def wkb_to_raster(wkb):
    try:
        (endian,) = unpack('B', wkb[0])
    except struct.error as e:
        eprint(e)
        raise RasterProcessingException("Could not unpack endianness!")

    if endian == 0:
        endian = '>'
    elif endian == 1:
        endian = '<'

    try:
        (version, n_bands, scale_X, scale_Y, ip_X, ip_Y, skew_X, skew_Y, srid, width, height) = unpack(
            endian + 'HHddddddiHH',
            wkb[1:61])
    except struct.error as e:
        eprint(e)
        raise RasterProcessingException("Could not unpack raster header!")

    raster = Raster(version, n_bands, scale_X, scale_Y, ip_X, ip_Y, skew_X, skew_Y, srid, width, height)

    for _ in range(n_bands):

        try:
            (bits,) = unpack(endian + 'b', wkb[61])
        except RasterProcessingException as e:
            eprint(e)
            raise RasterProcessingException("Could not unpack band header bits!")

        is_offline = bool(bits & 128)  # first bit
        has_no_data_value = bool(bits & 64)  # second bit
        is_no_data_value = bool(bits & 32)  # third bit

        pixtype = (bits & 15) - 1  # bits 4-8 TODO: why's it -1 here?

        fmts = ['?', 'B', 'B', 'b', 'B', 'h', 'H', 'i', 'I', 'f', 'd']
        dtypes = ['b1', 'u1', 'u1', 'i1', 'u1', 'i2', 'u2', 'i4', 'u4', 'f4', 'f8']
        sizes = [1, 1, 1, 1, 1, 2, 2, 4, 4, 4, 8]

        dtype = dtypes[pixtype]
        size = sizes[pixtype]
        fmt = fmts[pixtype]

        # Read the nodata value
        try:
            (nodata,) = unpack(endian + fmt, wkb[62:62+size])
        except struct.error as e:
            eprint(e)
            raise RasterProcessingException("Could not unpack band nodata value!")
        # Note that now data = data[height][width], i.e. height ~ row and width ~ column and
        # note that the data is filled in row-wise
        try:
            data = np.ndarray((height, width),
                              buffer=wkb[62+size:62+size+width*height*size],
                              dtype=np.dtype(dtype)
                              )
        except Exception as e:
            eprint(e)
            raise RasterProcessingException("Could not fill in actual data into numpy array!")

        band = Band(is_offline, has_no_data_value, is_no_data_value, pixtype, nodata, data)
        raster.add_band(band)

    return raster


def hexwkb_to_raster(hexwkb):
    wkb = hexwkb.decode("hex")
    return wkb_to_raster(wkb)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse wkb processing parameters.')
    parser.add_argument('--input', help='Input wkb file', required=True)
    parser.add_argument('--output', help='Output wkb file', required=True)
    args = parser.parse_args()
    with open(args.input, 'r') as fin:
        data_in = fin.read()
        raster = wkb_to_raster(data_in)
        data_out = raster.raster_to_wkb(1)
        with open(args.output, 'w') as fout:
            fout.write(data_out)