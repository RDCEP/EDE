import argparse
from netCDF4 import Dataset


def main(netcdf_filename):
    try:
        ds = Dataset(netcdf_filename, "r", format="NETCDF4")
        print netcdf_filename
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process raster configuration parameters.')
    parser.add_argument('--input', help='Input netcdf filename', required=True)
    args = parser.parse_args()
    main(args.input)