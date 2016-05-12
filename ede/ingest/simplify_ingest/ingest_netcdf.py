from __future__ import print_function
import sys
import argparse
from netCDF4 import Dataset


def eprint(*args, **kwargs):
    """Prints to stderr
    :param args:
    :param kwargs:
    :return:
    """
    print(*args, file=sys.stderr, **kwargs)


class RasterProcessingException(Exception):
    """Represents an exception that can occur during the processing of a raster file
    """
    def __init__(self, message):
        super(RasterProcessingException, self).__init__(message)


def get_bounding_box(longitudes, latitudes):
    """Returns the bounding box
    :param longitudes:
    :param latitudes:
    :return: The corners of the bounding box each corner being a tuple with first the longitude
    and then the latitude coordinate
    """
    lon_min = float(min(longitudes))
    lon_max = float(max(longitudes))
    lat_min = float(min(latitudes))
    lat_max = float(max(latitudes))
    return (((lon_min, lat_min), (lon_max, lat_min), (lon_max, lat_max), (lon_min, lat_max), (lon_min, lat_min)))


def get_longitudes_latitudes(dataset):
    """Get longitudes and latitudes arrays of dataset
    :param dataset:
    :return: Arrays of longitudes and latitudes
    """
    found = False

    if found is False:
        try:
            lons = dataset.variables['lon']
            lats = dataset.variables['lat']
            found = True
        except:
            pass

    if found is False:
        try:
            lons = dataset.variables['X']
            lats = dataset.variables['Y']
            found = True
        except:
            pass

    if found is False:
        try:
            lons = dataset.variables['longitude']
            lats = dataset.variables['latitude']
            found = True
        except:
            pass

    if found is False:
        raise RasterProcessingException("Could not find longitude and latitude in dataset")
    else:
        '''
        Note that this loads the longitudes and latitudes fully into memory.
        To save memory we might want to load them in chunks so return iterables here.
        '''
        return lons[:], lats[:]


def get_global_attributes(dataset):
    """ Returns the global attributes of the dataset
    :param dataset:
    :return: List of tuples (attribute key, attribute value)
    """
    attrs = []
    for attr_key in dataset.ncattrs():
        attrs.append((attr_key, dataset.getncattr(attr_key)))
    return attrs


def get_variables_info(dataset):
    """Returns information about the variables in the dataset
    :param dataset:
    :return: A list of dictionaries each storing name, datatype, number of dimensions, shape, dimensions, attributes
    of the variables
    """
    vars_info = []
    for var in dataset.variables.values():
        attributes = []
        for attr in var.ncattrs():
            attributes.append({
                "name": attr,
                "value": str(var.getncattr(attr))
            })
        # The variable's info
        vars_info.append({
            "name": var.name,
            "dtype": str(var.dtype),
            "ndim": var.ndim,
            "shape": var.shape,
            "dimensions": var.dimensions,
            "attributes": attributes
        })
    return vars_info


def get_dimensions_info(dataset):
    """Returns the dimensions and their sizes of the dataset
    :param dataset:
    :return: List of tuples (dimension, dimension size)
    """
    dims_info = []
    for dim in dataset.dimensions.values():
        dims_info.append((dim.name, dim.size))
    return dims_info


def process_netcdf(netcdf_filename):
    """Processes the NetCDF
    :param netcdf_filename:
    :return:
    """
    try:
        ds = Dataset(netcdf_filename, "r", format="NETCDF4")
    except IOError as e:
        eprint("I/O error({0}): {1}".format(e.errno, e.strerror))
        raise

    dims_info = get_dimensions_info(ds)
    vars_info = get_variables_info(ds)
    global_attrs = get_global_attributes(ds)

    try:
        longs, lats = get_longitudes_latitudes(ds)
    except:
        raise RasterProcessingException("Could not get longitudes and latitudes of netcdf file: %s", netcdf_filename)

    bbox = get_bounding_box(longs, lats)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process raster configuration parameters.')
    parser.add_argument('--input', help='Input netcdf filename', required=True)
    args = parser.parse_args()
    try:
        process_netcdf(args.input)
    except:
        eprint("Could not process netcdf file: %s", args.input)
        sys.exit()
