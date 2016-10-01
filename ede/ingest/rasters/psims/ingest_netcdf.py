import argparse
import tempfile
import os
import sys
import time
from datetime import datetime, timedelta
from itertools import izip
import numpy as np
import numpy.ma as ma
import psycopg2
from netCDF4 import Dataset
from psycopg2.extras import Json
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST
from ede.utils.raster import RasterProcessingException, Raster, Band
import json


def parse_metadata(filename):
    """Parses the metadata of the NetCDF
    Returned is the following dictionary:
    {
        "name": <string: filename>,
        "variables":
        [
            {
                "name": <string: name of variable>
                "dtype": <string: type of variable>
                "ndim": <integer: number of dimensions the variable depends on>
                "shape": <tuple[integer]: sizes of the dimensions the variable depends on>
                "dimensions": <list[string]: the names of the dimensions the variable depends on>
                "attributes":
                [
                    {
                        "name": <string: name of the variable's attribute>
                        "value": <string: value of the variable's attribute>
                    },
                    ...
                ]
            },
            ...
        ],
        "dimensions":
        {
            "lon":
            {
                "name": "lon"
                "dtype": <see for variables>
                "ndim": <see for variables>
                "shape": <see for variables>
                "dimensions": <see for variables>
                "attributes": <see for variables>
            },
            ...
        }
        "attributes":
        [
            {
                "name": <string: name of the global attribute>
                "value": <string: value of the global attribute>
            },
            ...
        ],
        "bbox":
        {
            "type": "Polygon",
            "coordinates": <list[list[list[float]]]: coordinates of bounding box>
        }
    }
    :param filename:
    :return:
    """

    ### Get meta data ###
    rootgrp = Dataset(filename, "r", format="NETCDF4")

    # The dimensions + variables
    dimensions = {}
    variables = []
    date_field_str = None
    for var in rootgrp.variables.values():
        # The dimensions the variable depends on
        dep_dims =[]
        for dim in var.dimensions:
            dep_dims.append(dim)
        # The attributes of the variable
        attributes=[]
        for attr in var.ncattrs():
            attributes.append({
                "name":attr,
                "value": str(var.getncattr(attr))
            })
            if var.name == 'time' and attr == 'units':
                date_field_str = str(var.getncattr(attr))
        # The variable's info
        var_info = {
            "name":var.name,
            "dtype":str(var.dtype),
            "ndim":var.ndim,
            "shape":var.shape,
            "dimensions":dep_dims,
            "attributes":attributes
        }
        if var.name in ['lon', 'lat', 'time', 'depth']:
            if var.name == 'lon':
                lons = rootgrp.variables['lon']
                lon_start = lons[0]
                lon_end = lons[-1]
                lon_step = lons[1] - lons[0]
                var_info['lon_start'] = lon_start
                var_info['lon_end'] = lon_end
                var_info['lon_step'] = lon_step
                dimensions['lon'] = var_info
            elif var.name == 'lat':
                lats = rootgrp.variables['lat']
                lat_start = lats[0]
                lat_end = lats[-1]
                lat_step = lats[1] - lats[0]
                var_info['lat_start'] = lat_start
                var_info['lat_end'] = lat_end
                var_info['lat_step'] = lat_step
                dimensions['lat'] = var_info
            elif var.name == 'time':
                # Get time_start, time_end, time_step
                date_fields_str = date_field_str.split("since")
                date_unit_str = date_fields_str[0].strip()
                if date_unit_str == "days":
                    var_info['time_step'] = '1 day'
                    date_delta = timedelta(days=1)
                elif date_unit_str == "growing seasons":
                    var_info['time_step'] = '1 year'
                    date_delta = timedelta(days=365)
                else:
                    raise "Got unexpected time unit!"
                date_start_str = date_fields_str[1].strip()
                date_start = datetime.strptime(date_start_str, "%Y-%m-%d %H:%M:%S")
                var_info['time_start'] = date_start.strftime("%Y-%m-%d %H:%M:%S")
                num_times = var_info['shape'][0]
                # TODO: this takes into account leap years which we don't want if the unit is 1 year!
                date_end = date_start + (num_times-1) * date_delta
                var_info['time_end'] = date_end.strftime("%Y-%m-%d %H:%M:%S")
                dimensions['time'] = var_info
            elif var.name == 'depth':
                raise "Depth dimension not yet supported!"
        else:
            variables.append(var_info)

    # The global attributes
    attributes = []
    for attr_key in rootgrp.ncattrs():
        attributes.append({
            "name":attr_key,
            "value":rootgrp.getncattr(attr_key)
        })

    meta_data = {
        "name": os.path.basename(filename),
        "variables": variables,
        "dimensions": dimensions,
        "attributes": attributes
    }

    # The bounding box
    lon1=float(min(rootgrp.variables['lon']))
    lon2=float(max(rootgrp.variables['lon']))
    lat1=float(min(rootgrp.variables['lat']))
    lat2=float(max(rootgrp.variables['lat']))

    meta_data["bbox"]={
        "type": "Polygon",
        "coordinates": [[[lon1,lat1],[lon2,lat1],[lon2,lat2],[lon1,lat2],[lon1,lat1]]]
    }

    rootgrp.close()

    return meta_data


def ingest_metadata(cur, dataset_metadata):

    short_name = "psims"
    long_name = "PSIMS - The parallel system for integrating impact models and sectors"
    attrs = {}
    for attr in dataset_metadata['attributes']:
        attrs[attr['name']] = attr['value']
    lon = dataset_metadata['dimensions']['lon']
    lon_start = lon['lon_start']
    lon_end = lon['lon_end']
    lon_step = lon['lon_step']
    num_lons = lon['shape'][0]
    lat = dataset_metadata['dimensions']['lat']
    lat_start = lat['lat_start']
    lat_end = lat['lat_end']
    lat_step = lat['lat_step']
    num_lats = lat['shape'][0]
    bbox = json.dumps(dataset_metadata['bbox'])
    times = dataset_metadata['dimensions']['time']
    time_start = times['time_start']
    time_end = times['time_end']
    time_step = times['time_step']
    num_times = times['shape'][0]
    # TODO: ugly, shouldn't be looping to get the attribute
    time_unit = None
    for attr in times['attributes']:
        if attr['name'] == 'units':
            time_unit = attr['value']

    query = ("INSERT INTO raster_datasets (short_name, long_name, "
             "lon_start, lon_end, lon_step, num_lons, "
             "lat_start, lat_end, lat_step, num_lats, bbox, "
             "time_start, time_end, time_step, num_times, time_unit, attrs) VALUES "
             "(\'{}\', \'{}\', "
             "{}, {}, {}, {}, "
             "{}, {}, {}, {}, ST_SetSRID(ST_GeomFromGeoJSON(\'{}\'),4326), "
             "\'{}\', \'{}\', \'{}\', {}, \'{}\', \'{}') RETURNING uid".
             format(short_name, long_name,
                    lon_start, lon_end, lon_step, num_lons,
                    lat_start, lat_end, lat_step, num_lats, bbox,
                    time_start, time_end, time_step, num_times, time_unit, attrs))

    cur.execute(query)
    (dataset_id,) = cur.fetchone()
    return dataset_id


def ingest_variable(cur, dataset_id, variable):

    query = ("INSERT INTO raster_variables (dataset_id, name, attrs) VALUES ({}, \'{}\', \'{}\') RETURNING uid".
             format(dataset_id, variable['name'], json.dumps(variable['attributes'])))
    cur.execute(query)
    (var_id,) = cur.fetchone()
    return var_id


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
        print(e)
        raise RasterProcessingException("Could not get meta_id for netcdf: {}".format(netcdf_filename))


def ingest_lat_lon(wkb_filename, cursor):
    try:
        cursor.execute("copy grid_data_lat_lon(meta_id, var_id, rast) from \'{}\'".format(wkb_filename))
    except Exception as e:
        print(e)
        raise RasterProcessingException("Could not ingest var(lat,lon)!")


def ingest_time_lat_lon(wkb_filename, cursor):
    try:
        cursor.execute("copy raster_data(dataset_id, var_id, rast) from \'{}\'".format(wkb_filename))
    except Exception as e:
        print(e)
        raise RasterProcessingException("Could not ingest var(time,lat,lon)!")


def ingest_depth_lat_lon(wkb_filename, cursor):
    try:
        cursor.execute("copy grid_data_depth_lat_lon(meta_id, var_id, depth_id, rast) from \'{}\'".format(wkb_filename))
    except Exception as e:
        print(e)
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
    return abs(res)


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


def process_band_lat_lon(variable, tile_size_lat, tile_size_lon, band_vals):
    """Processes a variable that depends on (lon,lat,depth)

    TODO: Make sure to handle all permutations correctly, i.e.
    (lon,lat,depth), (depth,lon,lat), etc.
    Right now we're assuming (depth,lat,lon)

    :param variable:
    :return:
    """
    assert (len(variable) == len(band_vals))
    num_tiles_lat = ceil_integer_division(360, tile_size_lat) # hardcoded to 360
    num_tiles_lon = ceil_integer_division(720, tile_size_lon) # hardcoded to 720
    for i in range(num_tiles_lat):
        for j in range(num_tiles_lon):
            for k in band_vals:
                yield i, j, k, variable[k][i * tile_size_lat: (i + 1) * tile_size_lat, j * tile_size_lon: (j + 1) * tile_size_lon]


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
            print("Variable {} depends only on {} which is not itself. "
                   "This case is not supported!"
                   .format(variable.name, dims[0]))
            raise RasterProcessingException("Variable {} depends only on {} which is not itself. "
                                            "This case is not supported!"
                                            .format(variable.name, dims[0]))
    elif num_dims == 2:
        # TODO: get the lon,lat strings at the very beginning when reading the file's metadata
        if 'lon' in dims and 'lat' in dims:
            # return process_lat_lon(variable, tile_size_lat, tile_size_lon)
            pass
        else:
            print("Variable {} depends on {} and {} which are not both spatial dimensions. "
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
                print("Variable {} depends on {}, {}, {} two of which are spatial "
                       "dimensions, but the third one is neither time nor depth. "
                       "This case is not supported!"
                       .format(variable.name, dims[0], dims[1], dims[2]))
                raise RasterProcessingException("Variable {} depends on {}, {}, {} two of which are spatial "
                                                "dimensions, but the third one is neither time nor depth. "
                                                "This case is not supported!"
                                                .format(variable.name, dims[0], dims[1], dims[2]))
        else:
            print("Variable {} depends on {}, {}, {} which don't contain two spatial"
                   "dimensions. This case is not supported!"
                   .format(variable.name, dims[0], dims[1], dims[2]))
            raise RasterProcessingException("Variable {} depends on {}, {}, {} which don't contain two spatial"
                                            "dimensions. This case is not supported!"
                                            .format(variable.name, dims[0], dims[1], dims[2]))
    else:
        print("Variable {} depends on more than 3 variables. This case is not supported!"
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
    :param wkb_filename:
    :return:
    """
    try:
        ds = Dataset(netcdf_filename, "r", format="NETCDF4")
    except IOError as e:
        print("I/O error({}): {}".format(e.errno, e.strerror))
        raise
    except Exception as e:
        print("Other problem during opening the dataset: {}".format(e))
        raise

    try:
        longs, lats = get_longitudes_latitudes(ds)
    except:
        raise RasterProcessingException(
            "Could not get longitudes and latitudes of netcdf file: {}".format(netcdf_filename))

    bbox = get_bounding_box(longs, lats)

    time_ids = range(1,35) # Hardcoded for PSIMS

    dataset_metadata = parse_metadata(netcdf_filename)

    # Note that x = longitude & y = latitude
    try:
        scale_X = get_resolution(longs)
    except RasterProcessingException as e:
        print(e)
        raise RasterProcessingException(
            "Could not get longitude resolution of netcdf file: {}".format(netcdf_filename))

    try:
        scale_Y = get_resolution(lats)
    except RasterProcessingException as e:
        print(e)
        raise RasterProcessingException(
            "Could not get latitude resolutions of netcdf file: {}".format(netcdf_filename))

    version = 0  # Always version = 0
    n_bands = 34  # We ingest packed rast fields
    ip_X_raster = min(longs) - 0.5 * scale_X
    ip_Y_raster = max(lats) + 0.5 * scale_Y
    # TODO: does a netcdf always have 0 skew?
    skew_X = 0.0
    skew_Y = 0.0
    # TODO: does a netcdf always have srid 4326?
    srid = 4326
    # TODO: make the tile sizes more easily choosable
    tile_size_lat = 60
    tile_size_lon = 60
    num_tiles_lat = ceil_integer_division(360, tile_size_lat) # for now code only works if divisible, TODO
    num_tiles_lon = ceil_integer_division(720, tile_size_lon) # for now code only works if divisible, TODO
    tile_width = tile_size_lon * scale_X
    tile_height = tile_size_lat * scale_Y
    is_offline = False
    has_no_data_value = True  # we're assuming there's always a NODATA value! TODO: maybe check if NODATA val is None
    is_no_data_value = False  # we're being conservative here
    num_times = 34 # hardcoded for PSIMS!

    conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS,
                            host=DB_HOST, port=DB_PORT)
    cur = conn.cursor()

    try:
        dataset_id = ingest_metadata(cur, dataset_metadata)
    except Exception as e:
        print(e)
        raise RasterProcessingException("Could not ingest metadata and get meta_id!")

    proper_vars = [var for var in ds.variables.values() if is_proper_variable(var)]

    try:
        for var in proper_vars:
            variable = next(v for v in dataset_metadata['variables'] if v['name'] == var.name)
            var_id = ingest_variable(cur, dataset_id, variable)
            pixtype = get_pixtype(var)
            try:
                nodata = var._FillValue
            except:
                nodata = get_nodata_value(pixtype)
            with open(wkb_filename, 'w') as f:
                for lat_tile_id in range(num_tiles_lat):
                    for lon_tile_id in range(num_tiles_lon):
                        ip_X_tile = ip_X_raster + lon_tile_id * tile_width
                        ip_Y_tile = ip_Y_raster - lat_tile_id * tile_height
                        rast = Raster(version, n_bands, scale_X, -scale_Y, ip_X_tile, ip_Y_tile, skew_X, skew_Y,
                                      srid, tile_size_lon, tile_size_lat)
                        for time in range(num_times):
                            tile = var[time][lat_tile_id * tile_size_lat: (lat_tile_id + 1) * tile_size_lat,
                                           lon_tile_id * tile_size_lon: (lon_tile_id + 1) * tile_size_lon]
                            band = Band(is_offline, has_no_data_value, is_no_data_value, pixtype, nodata, tile)
                            rast.add_band(band)
                        hexwkb = rast.raster_to_hexwkb(1)
                        row = compose_fields(dataset_id, var_id, None, hexwkb)
                        f.write(row + '\n')
                f.seek(-1, os.SEEK_END)
                f.truncate()
                ingest_actual_data(wkb_filename, cur, var)
    except RasterProcessingException as e:
        print(e)
        raise RasterProcessingException("process_netcdf: Could not process variables!")
    except Exception as e:
        print(e)
        raise RasterProcessingException("process_netcdf: Could not process variables!")

    conn.commit()


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
        print(e)
        print("Could not process netcdf file: {}".format(args.input))
        sys.exit()