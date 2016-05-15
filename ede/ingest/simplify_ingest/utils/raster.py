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
        self.is_offline = is_offline
        self.has_no_data_value = has_no_data_value
        self.is_no_data_value = is_no_data_value
        self.pixtype = pixtype
        self.nodata = nodata
        self.data = data


class Raster(object):
    def __init__(self, version, n_bands, scale_X, scale_Y, ip_X, ip_Y, skew_X, skew_Y, srid, width, height):
        self.version = version
        self.n_bands = n_bands
        self.scale_X = scale_X
        self.scale_Y = scale_Y
        self.ip_X = ip_X
        self.ip_Y = ip_Y
        self.skew_X = skew_X
        self.skew_Y = skew_Y
        self.srid = srid
        self.width = width
        self.height = height
        self.bands = []

    def add_band(self, band):
        self.bands.append(band)

    def raster_to_wkb(self, wkb_filename, endian):

        with open(wkb_filename, 'w') as f:

            try:
                buff = pack('B', endian)
            except struct.error as e:
                eprint(e)
                raise RasterProcessingException("Could not pack endian-ness!")

            f.write(buff)

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

            f.write(buff)

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

                f.write(buff)

                # Write out nodata value
                try:
                    buff = pack(endian + fmt, band.nodata)
                except struct.error as e:
                    eprint(e)
                    raise RasterProcessingException("Could not pack nodata value!")

                f.write(buff)

                # Write out actual data
                try:
                    buff = pack('{}{}{}'.format(endian, num_pixels, fmt), *band.data.flatten())
                except struct.error as e:
                    eprint(e)
                    raise RasterProcessingException("Could not pack actual band data!")

                f.write(buff)


def wkb_to_raster(wkb_filename):
    with open(wkb_filename, 'r') as f:

        try:
            (endian,) = unpack('B', f.read(1))
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
                f.read(60))
        except struct.error as e:
            eprint(e)
            raise RasterProcessingException("Could not unpack raster header!")

        raster = Raster(version, n_bands, scale_X, scale_Y, ip_X, ip_Y, skew_X, skew_Y, srid, width, height)

        for _ in range(n_bands):

            try:
                (bits,) = unpack(endian + 'b', f.read(1))
            except RasterProcessingException as e:
                eprint(e)
                raise RasterProcessingException("Could not unpack band header bits!")

            is_offline = bool(bits & 128)  # first bit
            has_no_data_value = bool(bits & 64)  # second bit
            is_no_data_value = bool(bits & 32)  # third bit

            pixtype = (bits & 15) - 1 # bits 4-8 TODO: why's it -1 here?

            fmts = ['?', 'B', 'B', 'b', 'B', 'h', 'H', 'i', 'I', 'f', 'd']
            dtypes = ['b1', 'u1', 'u1', 'i1', 'u1', 'i2', 'u2', 'i4', 'u4', 'f4', 'f8']
            sizes = [1, 1, 1, 1, 1, 2, 2, 4, 4, 4, 8]

            dtype = dtypes[pixtype]
            size = sizes[pixtype]
            fmt = fmts[pixtype]

            # Read the nodata value
            try:
                (nodata,) = unpack(endian + fmt, f.read(size))
            except struct.error as e:
                eprint(e)
                raise RasterProcessingException("Could not unpack band nodata value!")

            # Note that now data = data[height][width], i.e. height ~ row and width ~ column and
            # note that the data is filled in row-wise
            try:
                data = np.ndarray((height, width),
                              buffer=f.read(width * height * size),
                              dtype=np.dtype(dtype)
                              )
            except Exception as e:
                eprint(e)
                raise RasterProcessingException("Could not fill in actual data into numpy array!")

            band = Band(is_offline, has_no_data_value, is_no_data_value, pixtype, nodata, data)
            raster.add_band(band)

        return raster


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse wkb processing parameters.')
    parser.add_argument('--input', help='Input wkb file', required=True)
    parser.add_argument('--output', help='Output wkb file', required=True)
    args = parser.parse_args()
    raster = wkb_to_raster(args.input)
    raster.raster_to_wkb(args.output, 1)
