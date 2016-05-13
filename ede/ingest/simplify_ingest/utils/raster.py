from struct import unpack, unpack_from, pack
import numpy as np
import argparse


#
#
# class Band(object):
#
#     def __init__(self, wkb):
#         self.pixtype =
#         self.offline =
#         self.width =
#         self.height =
#         self.hasnodata =
#         self.isnodata =
#         self.nodataval =
#         self.ownsdata =
#         self.raster =
#         self.data =
#
#
# class Raster(object):
#
#     def __init__(selfs):
#         self.version =
#         self.nBands =
#         self.scaleX =
#         self.scaleY =
#         self.ipX =
#         self.ipY =
#         self.skewX =
#         self.skewY =
#         self.srid =
#         self.width =
#         self.height =

#         self.bands =

def deserialize_wkb(wkb_filename):
    with open(wkb_filename, 'r') as f:

        (endian,) = unpack('B', f.read(1))

        if endian == 0:
            endian = '>'
        elif endian == 1:
            endian = '<'

        (version, n_bands, scale_X, scale_Y, ip_X, ip_Y, skew_X, skew_Y, srid, width, height) = unpack(
            endian + 'HHddddddiHH',
            f.read(60))

        print("Raster header info...")
        print((version, n_bands, scale_X, scale_Y, ip_X, ip_Y, skew_X, skew_Y, srid, width, height))

        for _ in range(n_bands):
            (bits,) = unpack(endian + 'b', f.read(1))

            is_offline = bool(bits & 128)  # first bit
            has_no_data_value = bool(bits & 64)  # second bit
            is_no_data_value = bool(bits & 32)  # third bit

            pixtype = (bits & 15) - 1  # bits 5-8 TODO: don't think -1 is correct

            fmts = ['?', 'B', 'B', 'b', 'B', 'h', 'H', 'i', 'I', 'f', 'd']
            dtypes = ['b1', 'u1', 'u1', 'i1', 'u1', 'i2', 'u2', 'i4', 'u4', 'f4', 'f8']
            sizes = [1, 1, 1, 1, 1, 2, 2, 4, 4, 4, 8]

            dtype = dtypes[pixtype]
            size = sizes[pixtype]
            fmt = fmts[pixtype]

            # Read the nodata value
            (nodata,) = unpack(endian + fmt, f.read(size))

            # Note that now data = data[height][width], i.e. height ~ row and width ~ column and
            # note that the data is filled in row-wise
            data = np.ndarray((height, width),
                              buffer=f.read(width * height * size),
                              dtype=np.dtype(dtype)
                              )

            print("Band header info...")
            print((is_offline, has_no_data_value, is_no_data_value, pixtype, nodata))

            print("Band actual data...")
            print(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse wkb processing parameters.')
    parser.add_argument('--input', help='Input wkb file', required=True)
    args = parser.parse_args()
    deserialize_wkb(args.input)
