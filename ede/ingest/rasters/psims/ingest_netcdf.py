import numpy as np
import tempfile
import os, sys
from netCDF4 import Dataset
import argparse
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST
import psycopg2
import datetime
from datetime import datetime, timedelta
import json


def get_fill_value(variable):
    fill_value = None
    for attr in variable['attributes']:
        if attr['name'] == '_FillValue':
            fill_value = attr['value']
            break
    return fill_value


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
                try:
                    var_info['time_start'] = date_start.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    # Handle too old dates
                    var_info['time_start'] = date_start.isoformat(" ").split(".")[0]
                num_times = var_info['shape'][0]
                # TODO: this takes into account leap years which we don't want if the unit is 1 year!
                date_end = date_start + (num_times-1) * date_delta
                try:
                    var_info['time_end'] = date_end.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    # Handle too old dates
                    var_info['time_end'] = date_end.isoformat(" ").split(".")[0]
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
             "\'{}\', \'{}\', \'{}\', {}, \'{}\', \'{}\') RETURNING uid".
             format(short_name, long_name,
                    lon_start, lon_end, lon_step, num_lons,
                    lat_start, lat_end, lat_step, num_lats, bbox,
                    time_start, time_end, time_step, num_times, time_unit, json.dumps(attrs)))

    cur.execute(query)
    (dataset_id,) = cur.fetchone()
    return dataset_id


def ingest_variable(cur, dataset_id, variable):

    # the dataset_id is hardcoded to 1 in order to avoid post-processing within the db later
    # to set this id correctly which takes way longer than doing it here
    # TODO: instead of 1, set it to be the correct dataset_id
    query = ("INSERT INTO raster_variables (dataset_id, name, attrs) VALUES (1, \'{}\', \'{}\') RETURNING uid".
             format(variable['name'], json.dumps(variable['attributes'])))
    cur.execute(query)
    (var_id,) = cur.fetchone()
    return var_id


def ingest_data(cur, filename, dataset_id, var_name, var_id, var_fill_value):

    fh = Dataset(filename)

    lons = fh.variables['lon'][:]
    lats = fh.variables['lat'][:]

    values = fh.variables[var_name][:]

    # with tempfile.NamedTemporaryFile() as f:
    #     for slice_id, slice in enumerate(values):
    #         time_id = slice_id + 1
    #         for lat_id, slab in enumerate(slice):
    #             lat = lats[lat_id]
    #             for lon_id, mval in enumerate(slab):
    #                 lon = lons[lon_id]
    #                 if isinstance(mval, np.ma.core.MaskedConstant):
    #                     f.write("{}\t{}\tSRID=4326;POINT({} {})\t{}\t{}\n".format(1, var_id, lon, lat, time_id, "\N"))
    #                 else:
    #                     f.write("{}\t{}\tSRID=4326;POINT({} {})\t{}\t{}\n".format(1, var_id, lon, lat, time_id, mval))
    #     f.seek(0)
    #     cur.copy_from(f, 'raster_data_single', columns=('dataset_id', 'var_id', 'geom', 'time_id', 'value'))
    #     print("size of CSV file ingested into raster_data_single: {}".format(os.path.getsize(f.name)))

    with tempfile.NamedTemporaryFile() as f:
        fill_value = values.fill_value
        for (slice_id, lat_id, lon_id), value in np.ndenumerate(values):
            time_id = slice_id + 1
            lat = lats[lat_id]
            lon = lons[lon_id]
            if value == fill_value:
                f.write("{}\t{}\tSRID=4326;POINT({} {})\t{}\t{}\n".format(1, var_id, lon, lat, time_id, "\N"))
            else:
                f.write("{}\t{}\tSRID=4326;POINT({} {})\t{}\t{}\n".format(1, var_id, lon, lat, time_id, mval))
        f.seek(0)
        cur.copy_from(f, 'raster_data_single', columns=('dataset_id', 'var_id', 'geom', 'time_id', 'value'))
        print("size of CSV file ingested into raster_data_single: {}".format(os.path.getsize(f.name)))

    # # also ingest into raster_data_series here instead of doing it later within DB which is much slower
    # with tempfile.NamedTemporaryFile() as f:
    #     num_lats = values.shape[1]
    #     num_lons = values.shape[2]
    #     for i_lat in range(num_lats):
    #         lat = lats[i_lat]
    #         for i_lon in range(num_lons):
    #             lon = lons[i_lon]
    #             values_slab = values[:, i_lat, i_lon]
    #             values_slab_conv = ["NULL" if isinstance(mval, np.ma.core.MaskedConstant) else str(mval) for mval in values_slab]
    #             # the dataset_id is hardcoded to 1 here in order to prevent having to set the dataset_id
    #             # correctly later within the DB using SQL which is way slower
    #             values_array_converted_str = ','.join(values_slab_conv)
    #             # TODO: instead of 1, use the correct dataset_id here
    #             f.write("{}\t{}\tSRID=4326;POINT({} {})\t{{{}}}\n".format(1, var_id, lon, lat, values_array_converted_str))
    #     # if this is not here, the copy_from below will succeed yet not ingest anything, very bad
    #     # TODO: protect this better
    #     f.seek(0)
    #     cur.copy_from(f, 'raster_data_series', columns=('dataset_id', 'var_id', 'geom', 'values'))
    #     print("size of CSV file ingested into raster_data_series: {}".format(os.path.getsize(f.name)))

    # also ingest into raster_data_series here instead of doing it later within DB which is much slower
    with tempfile.NamedTemporaryFile() as f:
        fill_value = values.fill_value
        num_lats = values.shape[1]
        num_lons = values.shape[2]
        for (i_lat, ), lat in range(num_lats):
            for (i_lon, ), lon in range(num_lons):
                values_slab = values[:, i_lat, i_lon]
                values_slab_conv = [ "NULL" if val == fill_value else str(val) for _, val in np.ndenumerate(values_slab)]
                values_array_converted_str = ','.join(values_slab_conv)
                # the dataset_id is hardcoded to 1 here in order to prevent having to set the dataset_id
                # correctly later within the DB using SQL which is way slower
                # TODO: instead of 1, use the correct dataset_id here
                f.write("{}\t{}\tSRID=4326;POINT({} {})\t{{{}}}\n".format(1, var_id, lon, lat, values_array_converted_str))
        # if this is not here, the copy_from below will succeed yet not ingest anything, very bad
        # TODO: protect this better
        f.seek(0)
        cur.copy_from(f, 'raster_data_series', columns=('dataset_id', 'var_id', 'geom', 'values'))
        print("size of CSV file ingested into raster_data_series: {}".format(os.path.getsize(f.name)))


def ingest_netcdf(filename):

    ## Connection to the database ##
    conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS,
                            host=DB_HOST, port=DB_PORT)
    cur = conn.cursor()

    dataset_metadata = parse_metadata(filename)
    dataset_id = ingest_metadata(cur, dataset_metadata)

    for variable in dataset_metadata['variables']:
        var_id = ingest_variable(cur, dataset_id, variable)
        var_name = variable['name']
        var_fill_value = get_fill_value(variable)
        ingest_data(cur, filename, dataset_id, var_name, var_id, var_fill_value)

    conn.commit()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Arguments for ingesting NetCDF')
    parser.add_argument('--input', help='Input NetCDF', required=True)
    args = parser.parse_args()
    try:
        ingest_netcdf(args.input)
    except Exception as e:
        print(e)
        print("Could not ingest NetCDF: {}".format(args.input))
        sys.exit()