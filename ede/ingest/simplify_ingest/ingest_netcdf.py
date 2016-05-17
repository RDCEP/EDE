import sys, os
import argparse
from netCDF4 import Dataset
import numpy as np
import numpy.ma as ma
from ede.ingest.simplify_ingest.utils.raster import *
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import Json
from psycopg2 import DatabaseError
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST
from itertools import izip


def insert_get_var_id(cursor, variable):
    try:
        # check if variable already there
        cursor.execute("select uid from grid_vars where vname = \'{}\'".format(variable.name))
        rows = cursor.fetchall()
        if not rows:
            # insert if variable not already there
            cursor.execute("insert into grid_vars (vname) values (\'{}\') returning uid".format(variable.name))
            rows = cursor.fetchall()
        for row in rows:
            var_id = int(row[0])
            return var_id
    except DatabaseError as e:
        eprint(e)
        raise RasterProcessingException("Could not get variable id for variable: {}. Due to DatabaseError"
                                        .format(variable.name))
    except Exception as e:
        eprint(e)
        raise RasterProcessingException("Could not get variable id for variable: {}".format(variable.name))


def insert_get_meta_id(cursor, netcdf_filename, meta_data):
    try:
        cursor.execute(
            "insert into grid_meta (filename, filesize, filetype, meta_data, date_created) values "
            "(\'{}\', {}, \'{}\', {}, \'{}\') returning uid".format(
                os.path.basename(netcdf_filename), os.path.getsize(netcdf_filename), 'HDF', Json(meta_data),
                time.ctime(os.path.getctime(netcdf_filename))))
        rows = cursor.fetchall()
        for row in rows:
            meta_id = int(row[0])
            return meta_id
    except Exception as e:
        eprint(e)
        raise RasterProcessingException("Could not get meta_id for netcdf: {}".format(netcdf_filename))


def ingest_lat_lon(wkb_filename, cursor):
    try:
        cursor.execute("copy grid_data_lat_lon(meta_id, var_id, rast) from \'{}\'".format(wkb_filename))
    except Exception as e:
        eprint(e)
        raise RasterProcessingException("Could not ingest var(lat,lon)!")


def ingest_time_lat_lon(wkb_filename, cursor):
    try:
        cursor.execute("copy grid_data_time_lat_lon(meta_id, var_id, time_id, rast) from \'{}\'".format(wkb_filename))
    except Exception as e:
        eprint(e)
        raise RasterProcessingException("Could not ingest var(time,lat,lon)!")


def ingest_depth_lat_lon(wkb_filename, cursor):
    try:
        cursor.execute("copy grid_data_depth_lat_lon(meta_id, var_id, depth_id, rast) from \'{}\'".format(wkb_filename))
    except Exception as e:
        eprint(e)
        raise RasterProcessingException("Could not ingest var(depth,lat,lon)!")


def ingest_actual_data(wkb_filename, cursor, variable):
    dims = variable.dimensions
    num_dims = len(dims)
    if num_dims == 2:
        ingest_lat_lon(wkb_filename, cursor)
    elif num_dims == 3:
        if 'time' in dims:
            ingest_time_lat_lon(wkb_filename, cursor)
        elif 'depth' in dims:
            ingest_depth_lat_lon(wkb_filename, cursor)
        else:
            raise RasterProcessingException("Got variable with unexpected dimensions when ingesting actual data!")
    else:
        raise RasterProcessingException("Got variable with unexpected dimensions when ingesting actual data!")


def compose_fields(meta_id, var_id, band_id, hexwkb):
    """Writes into file + calls postgres' copy from on it, for the right table
    i.e. raster_time_lat_lon / raster_depth_lat_lon / raster_lat_lon

    TODO: again need to do the logic here to determine if it depends on depth/time.
    get rid of this redundancy, and somehow do it already in process_variable only
    :param meta_id:
    :param var_id:
    :param band_id:
    :param hexwkb:
    :return:
    """
    if band_id is None:
        return "{}\t{}\t{}".format(meta_id, var_id, hexwkb)
    else:
        return "{}\t{}\t{}\t{}".format(meta_id, var_id, band_id, hexwkb)


def ceil_integer_division(a, b):
    return (a + b - 1) // b


def get_pixtype(variable):
    dtypes = map(np.dtype, ['b1', 'u1', 'u1', 'i1', 'u1', 'i2', 'u2', 'i4', 'u4', 'f4', 'f8'])
    return dtypes.index(variable.dtype)


def get_nodata_value(pixtype):
    dtypes = ['b1', 'u1', 'u1', 'i1', 'u1', 'i2', 'u2', 'i4', 'u4', 'f4', 'f8']
    return ma.default_fill_value(np.dtype(dtypes[pixtype]))


def get_resolution(array):
    # eps = np.finfo(float).eps # too strict, soil data wouldn't pass this
    eps = 1e-4
    res = array[1] - array[0]
    for i in range(1, len(array) - 1):
        res_next = array[i + 1] - array[i]
        if abs(res_next - res) > eps:
            raise RasterProcessingException("Does not have a uniform resolution at index {}. "
                                            "This case is not supported!".format(i))
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


def get_depth_ids(cursor, dataset, meta_id):
    """
    TODO: this is too slow => use executemany or something
    :param cursor:
    :param dataset:
    :param meta_id:
    :return:
    """
    try:
        depths = dataset.variables['depth'][:]
        depth_ids = []
        for depth in depths:
            cursor.execute("insert into grid_depths (meta_id, depth) values ({}, \'{}\') returning uid"
                           .format(meta_id, depth))
            rows = cursor.fetchall()
            for row in rows:
                depth_id = int(row[0])
                depth_ids.append(depth_id)
                break
    except:
        depth_ids = None
    return depth_ids


def get_time_ids(cursor, dataset, meta_id):
    """
    TODO: this is too slow => use executemany or something
    :param cursor:
    :param dataset:
    :param meta_id:
    :return:
    """
    try:
        times = dataset.variables['time'][:]
        time_ids = []
        for time in times:
            cursor.execute("insert into grid_times (meta_id, time) values ({}, \'{}\') returning uid"
                           .format(meta_id, time))
            rows = cursor.fetchall()
            for row in rows:
                time_id = int(row[0])
                time_ids.append(time_id)
                break
    except:
        time_ids = None
    return time_ids


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
        attrs.append({
            "name": attr_key,
            "value": dataset.getncattr(attr_key)
        })
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
        dims_info.append({
            "name": dim.name,
            "size": dim.size
        })
    return dims_info


def get_metadata(dataset, netcdf_filename, bbox):
    return {
        "name": os.path.basename(netcdf_filename),
        "dimensions": get_dimensions_info(dataset),
        "variables": get_variables_info(dataset),
        "attributes": get_global_attributes(dataset),
        "loc": {"type": "Polygon",
                "coordinates": bbox}
    }


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
            yield None, band[i * tile_size_lat: (i + 1) * tile_size_lat, j * tile_size_lon: (j + 1) * tile_size_lon]


def process_band_lat_lon(variable, tile_size_lat, tile_size_lon, band_vals):
    """Processes a variable that depends on (lon,lat,depth)

    TODO: Make sure to handle all permutations correctly, i.e.
    (lon,lat,depth), (depth,lon,lat), etc.
    Right now we're assuming (depth,lat,lon)

    :param variable:
    :return:
    """
    assert (len(variable) == len(band_vals))
    for band_val, band in izip(band_vals, variable):
        tiles = process_band(band, tile_size_lat, tile_size_lon)
        for tile in tiles:
            yield band_val, tile


def process_lat_lon(variable, tile_size_lat, tile_size_lon):
    """Processes a variable that depends on (lon,lat)

    TODO: Make sure to handle both (lon,lat) and (lat,lon) correctly
    Right now we're assuming (lat,lon)

    :param variable:
    :return:
    """
    return process_band(variable[:], tile_size_lat, tile_size_lon)


def process_variable(variable, tile_size_lat, tile_size_lon, times, depths):
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
                assert times is not None
                return process_band_lat_lon(variable, tile_size_lat, tile_size_lon, times)
            elif 'depth' in dims:
                assert depths is not None
                return process_band_lat_lon(variable, tile_size_lat, tile_size_lon, depths)
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

    try:
        longs, lats = get_longitudes_latitudes(ds)
    except:
        raise RasterProcessingException(
            "Could not get longitudes and latitudes of netcdf file: {}".format(netcdf_filename))

    bbox = get_bounding_box(longs, lats)

    meta_data = get_metadata(ds, netcdf_filename, bbox)

    # Note that x = longitude & y = latitude
    try:
        scale_X = get_resolution(longs)
    except RasterProcessingException as e:
        eprint(e)
        raise RasterProcessingException(
            "Could not get longitude resolution of netcdf file: {}".format(netcdf_filename))

    try:
        scale_Y = get_resolution(lats)
    except RasterProcessingException as e:
        eprint(e)
        raise RasterProcessingException(
            "Could not get latitude resolutions of netcdf file: {}".format(netcdf_filename))

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

    conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS,
                            host=DB_HOST, port=DB_PORT)
    cur = conn.cursor()

    try:
        meta_id = insert_get_meta_id(cur, netcdf_filename, meta_data)
    except Exception as e:
        eprint(e)
        raise RasterProcessingException("Could not ingest metadata and get meta_id!")

    time_ids = get_time_ids(cur, ds, meta_id)
    depth_ids = get_depth_ids(cur, ds, meta_id)

    proper_vars = [var for var in ds.variables.values() if is_proper_variable(var)]

    try:

        for var in proper_vars:
            var_id = insert_get_var_id(cur, var)
            pixtype = get_pixtype(var)
            tiles = process_variable(var, tile_size_lat, tile_size_lon, time_ids, depth_ids)
            try:
                nodata = var._FillValue
            except:
                nodata = get_nodata_value(pixtype)
            with open(wkb_filename, 'w') as f:
                for (band_id, tile) in tiles:
                    rast = Raster(version, n_bands, scale_X, scale_Y, ip_X, ip_Y, skew_X, skew_Y,
                                  srid, tile.shape[1], tile.shape[0])
                    band = Band(is_offline, has_no_data_value, is_no_data_value, pixtype, nodata, tile)
                    rast.add_band(band)
                    # TODO: make it return wkb byte buffer instead of already writing to file => be agnostic
                    hexwkb = rast.raster_to_hexwkb(1)
                    row = compose_fields(f, var, meta_id, var_id, band_id, hexwkb)
                    f.write(row + '\n')
            ingest_actual_data(wkb_filename, cur, var)

    except RasterProcessingException as e:
        eprint(e)
        raise RasterProcessingException("process_netcdf: Could not process variables!")
    except Exception as e:
        eprint(e)
        raise RasterProcessingException("process_netcdf: Could not process variables!")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse raster processing parameters.')
    parser.add_argument('--input', help='Input netcdf filename', required=True)
    parser.add_argument('--output', help='Output wkb filename', required=True)
    args = parser.parse_args()
    try:
        startTime = datetime.now()
        process_netcdf(args.input, args.output)
        print("Duration: {}".format(datetime.now() - startTime))
    except Exception as e:
        eprint(e)
        eprint("Could not process netcdf file: {}".format(args.input))
        sys.exit()
