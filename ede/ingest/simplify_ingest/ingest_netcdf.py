from __future__ import print_function
import sys
import argparse
from netCDF4 import Dataset


class RasterProcessingException(Exception):
    """Represents an exception that can occur during the processing of a raster file
    """

    def __init__(self, message):
        super(RasterProcessingException, self).__init__(message)


def ceil_integer_division(a, b):
    return a + b - 1 // b

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


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


def process_tile(tile, band_dim):
    """Processes a single tile
    :param tile:
    :param band_dim:
    :return:
    """
    print(tile)
    print(band_dim)


def process_band(band, band_dim):
    """Processes a single band

    Note that we are assuming band = band[lat][lon]

    :param band:
    :param band_dim:
    :return:
    """
    print("processing band...")
    # TODO: Don't hardcode the tilesizes
    tile_size_lat = 10
    tile_size_lon = 10
    band_shape = band.shape
    print("band shape: {}".format(band_shape))
    num_tiles_lat = ceil_integer_division(band_shape[0] / tile_size_lat)
    num_tiles_lon = ceil_integer_division(band_shape[1] / tile_size_lon)
    print("number of lat tiles: {}".format(num_tiles_lat))
    print("number of lon tiles: {}".format(num_tiles_lon))
    for i in range(num_tiles_lat):
        for j in range(num_tiles_lon):
            print(type(band))
            print("value: {}".format(band[0][0]))
            tile = band[i*tile_size_lat : (i+1)*tile_size_lat][j*tile_size_lon : (j+1)*tile_size_lon]
            print("begin processing of tile...")
            process_tile(tile, band_dim)


def process_depth_lat_lon(variable):
    """Processes a variable that depends on (lon,lat,depth)

    TODO: Make sure to handle all permutations correctly, i.e.
    (lon,lat,depth), (depth,lon,lat), etc.
    Right now we're assuming (depth,lat,lon)

    :param variable:
    :return:
    """
    for depth_band in variable:
        process_band(depth_band, 'depth')


def process_time_lat_lon(variable):
    """Processes a variable that depends on (lon,lat,time)

    TODO: Make sure to handle all permutations correctly, i.e.
    (lon,lat,time), (time,lon,lat), etc.
    Right now we're assuming (time,lat,lon)

    :param variable:
    :return:
    """
    try:
        for time_band in variable:
            process_band(time_band, 'time')
    except Exception as e:
        eprint("process_time_lat_lon: Could not process variable {}. Error: {}".format(variable.mame, e))
        raise

def process_lat_lon(variable):
    """Processes a variable that depends on (lon,lat)

    TODO: Make sure to handle both (lon,lat) and (lat,lon) correctly
    Right now we're assuming (lat,lon)

    :param variable:
    :return:
    """
    process_band(variable[:], None)


def process_variable(variable):
    """Processes an individual variable
    :param variable:
    :return:
    """
    dims = variable.dimensions
    num_dims = len(dims)
    if num_dims == 1:
        if variable.name == dims[0]:
            pass
        else:
            eprint("Variable {} depends only on {} which is not itself. "
                   "This case is not supported!"
                   .format(variable.name, dims[0]))
            raise RasterProcessingException("Variable {} depends only on {} which is not itself. "
                                            "This case is not supported!"
                                            .format(variable.name, dims[0]))
    elif num_dims == 2:
        # TODO: get the lon,lat strings at the very beginning when reading the file's metadata
        if 'lon' in dims and 'lat' in dims:
            try:
                process_lat_lon(variable)
            except:
                eprint("Could not process variable {} with process_lat_lon".format(variable.name))
                raise RasterProcessingException("Could not process variable {} with process_lat_lon"
                                                .format(variable.name))
        else:
            eprint("Variable {} depends on {} and {} which are not both spatial dimensions. "
                   "This case is not supported!"
                   .format(variable.name, dims[0], dims[1]))
            raise RasterProcessingException("Variable {} depends on {} and {} which are not both spatial dimensions. "
                                            "This case is not supported!"
                                            .format(variable.name, dims[0], dims[1]))
    elif num_dims == 3:
        # TODO: see just above
        if 'lon' in dims and 'lat' in dims:
            if 'time' in dims:
                try:
                    process_time_lat_lon(variable)
                except:
                    eprint("Could not process variable {} with process_time_lat_lon".format(variable.name))
                raise RasterProcessingException("Could not process variable {} with process_time_lat_lon"
                                                .format(variable.name))
            elif 'depth' in dims:
                try:
                    process_depth_lat_lon(variable)
                except:
                    eprint("Could not process variable {} with process_depth_lat_lon".format(variable.name))
                raise RasterProcessingException("Could not process variable {} with process_depth_lat_lon"
                                                .format(variable.name))
            else:
                eprint("Variable {} depends on {}, {}, {} two of which are spatial "
                       "dimensions, but the third one is neither time nor depth. "
                       "This case is not supported!"
                       .format(variable.name, dims[0], dims[1], dims[2]))
                raise RasterProcessingException("Variable {} depends on {}, {}, {} two of which are spatial "
                                                "dimensions, but the third one is neither time nor depth. "
                                                "This case is not supported!"
                                                .format(variable.name, dims[0], dims[1], dims[2]))
        else:
            eprint("Variable {} depends on {}, {}, {} which don't contain two spatial"
                   "dimensions. This case is not supported!"
                   .format(variable.name, dims[0], dims[1], dims[2]))
            raise RasterProcessingException("Variable {} depends on {}, {}, {} which don't contain two spatial"
                                            "dimensions. This case is not supported!"
                                            .format(variable.name, dims[0], dims[1], dims[2]))
    else:
        eprint("Variable {} depends on more than 3 variables. This case is not supported!"
               .format(variable.name))
        raise RasterProcessingException("Variable {} depends on more than 3 variables. This case is not supported!"
                                        .format(variable.name))


def is_proper_variable(variable):
    """Returns True if the variable is a proper variable
    :param variable:
    :return:
    """
    improper_vars = ['lon', 'longitude', 'X', 'lat', 'latitude', 'Y', 'time', 'depth']
    return variable.name not in improper_vars


def process_netcdf(netcdf_filename):
    """Processes the NetCDF
    :param netcdf_filename:
    :return:
    """
    try:
        ds = Dataset(netcdf_filename, "r", format="NETCDF4")
    except IOError as e:
        eprint("I/O error({}): {}".format(e.errno, e.strerror))
        raise
    except Exception as e:
        eprint("Other problem during opening the dataset: {}".format(e))
        raise

    # dims_info = get_dimensions_info(ds)
    # vars_info = get_variables_info(ds)
    # global_attrs = get_global_attributes(ds)
    #
    # try:
    #     longs, lats = get_longitudes_latitudes(ds)
    # except:
    #     raise RasterProcessingException("Could not get longitudes and latitudes of netcdf file: %s", netcdf_filename)
    #
    # bbox = get_bounding_box(longs, lats)

    proper_vars = [var for var in ds.variables.values() if is_proper_variable(var)]

    try:
        for var in proper_vars:
            process_variable(var)
    except:
        eprint("Could not process variables!")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse raster processing parameters.')
    parser.add_argument('--input', help='Input netcdf filename', required=True)
    args = parser.parse_args()
    try:
        process_netcdf(args.input)
    except:
        eprint("Could not process netcdf file: {}".format(args.input))
        sys.exit()
