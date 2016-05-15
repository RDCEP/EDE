import sys
import argparse
from netCDF4 import Dataset
import numpy as np
from ede.ingest.simplify_ingest.utils.raster import Raster, Band, eprint, RasterProcessingException


def ceil_integer_division(a, b):
    return (a + b - 1) // b


def get_pixtype(variable):
    dtypes = map(np.dtype, ['b1', 'u1', 'u1', 'i1', 'u1', 'i2', 'u2', 'i4', 'u4', 'f4', 'f8'])
    return dtypes.index(variable.dtype)


def get_resolution(array):
    eps = np.finfo(float).eps
    res = array[1] - array[0]
    for i in range(1, len(array) - 1):
        res_next = array[i + 1] - array[i]
        if abs(res_next - res) > eps:
            raise RasterProcessingException("Does not have a uniform resolution. This case is not supported!")
    return res


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


def process_band(band, tile_size_lat, tile_size_lon):
    """Processes a single band

    Note that we are assuming band = band[lat][lon]

    :param band:
    :param band_dim:
    :return:
    """
    band_shape = band.shape
    num_tiles_lat = ceil_integer_division(band_shape[0], tile_size_lat)
    num_tiles_lon = ceil_integer_division(band_shape[1], tile_size_lon)
    for i in range(num_tiles_lat):
        for j in range(num_tiles_lon):
            yield band[i * tile_size_lat: (i + 1) * tile_size_lat, j * tile_size_lon: (j + 1) * tile_size_lon]


def process_band_lat_lon(variable, tile_size_lat, tile_size_lon):
    """Processes a variable that depends on (lon,lat,depth)

    TODO: Make sure to handle all permutations correctly, i.e.
    (lon,lat,depth), (depth,lon,lat), etc.
    Right now we're assuming (depth,lat,lon)

    :param variable:
    :return:
    """
    for band in variable:
        tiles = process_band(band, tile_size_lat, tile_size_lon)
        for tile in tiles:
            yield tile


def process_lat_lon(variable, tile_size_lat, tile_size_lon):
    """Processes a variable that depends on (lon,lat)

    TODO: Make sure to handle both (lon,lat) and (lat,lon) correctly
    Right now we're assuming (lat,lon)

    :param variable:
    :return:
    """
    return process_band(variable[:], tile_size_lat, tile_size_lon)


def process_variable(variable, tile_size_lat, tile_size_lon):
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
            return process_lat_lon(variable, tile_size_lat, tile_size_lon)
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
                return process_band_lat_lon(variable, tile_size_lat, tile_size_lon)
            elif 'depth' in dims:
                return process_band_lat_lon(variable, tile_size_lat, tile_size_lon)
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


def process_netcdf(netcdf_filename, wkb_filename):
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

    dims_info = get_dimensions_info(ds)
    vars_info = get_variables_info(ds)
    global_attrs = get_global_attributes(ds)

    try:
        longs, lats = get_longitudes_latitudes(ds)
    except:
        raise RasterProcessingException(
            "Could not get longitudes and latitudes of netcdf file: {}".format(netcdf_filename))

    bbox = get_bounding_box(longs, lats)

    # Note that x = longitude & y = latitude
    try:
        scale_X = get_resolution(longs)
        scale_Y = get_resolution(lats)
    except RasterProcessingException as e:
        eprint(e)
        raise RasterProcessingException(
            "Could not get longitude and latitude resolutions of netcdf file: {}".format(netcdf_filename))

    version = 0  # Always version = 0
    n_bands = 1  # We ingest unpacked rast fields
    ip_X = longs[0] - 0.5 * scale_X
    ip_Y = lats[0] - 0.5 * scale_Y
    # TODO: does a netcdf always have 0 skew?
    skew_X = 0.0
    skew_Y = 0.0
    # TODO: does a netcdf always have srid 4326?
    srid = 4326
    tile_size_lat = 100
    tile_size_lon = 100

    is_offline = False
    has_no_data_value = True  # we're assuming there's always a no_data value! TODO: maybe check if nodata val is None
    is_no_data_value = False  # we're being conservative here

    proper_vars = [var for var in ds.variables.values() if is_proper_variable(var)]

    try:
        for var in proper_vars:
            pixtype = get_pixtype(var)
            print("hello there")
            nodata = var._FillValue # we're assuming this is not None!!
            tiles = process_variable(var, tile_size_lat, tile_size_lon)
            print("pixtype: {}".format(pixtype))
            print("nodata: {}".format(nodata))
            for tile in tiles:
                rast = Raster(version, n_bands, scale_X, scale_Y, ip_X, ip_Y, skew_X, skew_Y,
                              srid, tile.shape[1], tile.shape[0])
                band = Band(is_offline, has_no_data_value, is_no_data_value, pixtype, nodata, tile)
                rast.add_band(band)
                # TODO: make it return wkb byte buffer instead of already writing to file => be agnostic
                rast.raster_to_wkb(wkb_filename, 1)
                break

    except RasterProcessingException as e:
        eprint(e)
        raise RasterProcessingException("process_netcdf: Could not process variables!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse raster processing parameters.')
    parser.add_argument('--input', help='Input netcdf filename', required=True)
    parser.add_argument('--output', help='Output wkb filename', required=True)
    args = parser.parse_args()
    try:
        process_netcdf(args.input, args.output)
    except:
        eprint("Could not process netcdf file: {}".format(args.input))
        sys.exit()
