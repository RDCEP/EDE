import argparse
import sys, os
from netCDF4 import Dataset
import psycopg2
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST
import json
import numpy as np
import datetime


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)


def validate_date(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
        return True
    except ValueError:
        return False


def get_coord_var(rootgrp, dim_name):
    try:
        coord_var = rootgrp.variables[dim_name]
        if coord_var.ndim == 1 and dim_name in coord_var.dimensions:
            return coord_var
        else:
            return None
    except KeyError:
        return None


def get_coord_type_of_var(var):
    try:
        units = None
        positive = None
        for attr in var.ncattrs():
            if attr == "units":
                units = var.getncattr(attr)
            elif attr == "positive" and var.getncattr(attr) in ["up", "down", "Up", "Down"]:
                positive = var.getncattr(attr)
        var_type = None
        if units in ["bar", "millibar", "decibar", "atmosphere", "atm", "pascal", "Pa", "hPa"] or positive:
            var_type = "Z"
        elif units == "degrees_north":
            var_type = "Y"
        elif units == "degrees_east":
            var_type = "X"
        elif units:
            if "since" in units:
                units_comps = map(str.strip, map(str, units.split("since")))
                if len(units_comps) == 2:
                    (time_units, time_ref) = units_comps
                    time_units_valid = time_units in ["d", "day", "days",
                                                      "h", "hr", "hour", "hrs", "hours",
                                                      "min", "minute", "mins", "minutes",
                                                      "s", "sec", "second", "secs", "seconds"]
                    time_ref_valid = validate_date(time_ref)
                    if time_units_valid and time_ref_valid:
                        var_type = "T"
        return var_type
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        raise Exception("get_coord_type_of_var: Could not get coordinate variable of var: {}".format(var.name))


def get_time_interval_and_ref(time_var):
    try:
        units = time_var.getncattr("units")
        (time_units, time_ref) = map(str.strip, map(str, units.split("since")))
        time_ref = datetime.datetime.strptime(time_ref, '%Y-%m-%d %H:%M:%S')
        if time_units in ["d", "day", "days"]:
            time_interval = datetime.timedelta(days=1)
        elif time_units in ["h", "hr", "hour", "hrs", "hours"]:
            time_interval = datetime.timedelta(hours=1)
        elif time_units in ["min", "minute", "mins", "minutes"]:
            time_interval = datetime.timedelta(minutes=1)
        elif time_units in ["s", "sec", "second", "secs", "seconds"]:
            time_interval = datetime.timedelta(seconds=1)
        else:
            raise Exception("get_time_interval_and_ref: got unexpected time_units!!")
        return (time_interval, time_ref)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        raise Exception("get_time_interval_and_ref: Could not get time interval + "
                        "reference date of time variable: {}".format(time_var.name))


def ingest_netcdf(filename):

    batch_size_max = 2000
    batch_size_curr = 0
    batch = ""

    rootgrp = Dataset(filename, "r", format="NETCDF4")

    # Make sure we don't have a multigroup NetCDF
    if rootgrp.groups:
        raise Exception("Cannot yet handle multigroup NetCDFs!")

    # Get the global attributes of the netcdf
    attrs = []
    for attr_key in rootgrp.ncattrs():
        attrs.append({
            "name": attr_key,
            "value": rootgrp.getncattr(attr_key)
        })

    conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS,
                            host=DB_HOST, port=DB_PORT)
    cur = conn.cursor()

    try:
        # insert dataset + get uid
        cur.execute("INSERT INTO dataset (attrs) "
                    "VALUES (\'{}\') "
                    "RETURNING uid".
                    format(json.dumps(attrs, cls=MyEncoder)))
        (dataset_id,) = cur.fetchone()

        for var in rootgrp.variables.values():
            dims_names = '{' + ','.join(['\"{}\"'.format(dim) for dim in var.dimensions]) + '}'
            dims_sizes = '{' + ','.join(map(str,var.shape)) + '}'
            attrs = []
            units = None
            positive = None
            coordinates = None
            fill_value = None
            missing_value = None
            valid_min = None
            valid_max = None
            valid_range = None
            add_offset = None
            scale_factor = None
            for attr in var.ncattrs():
                attrs.append({
                    "name": attr,
                    "value": var.getncattr(attr)
                })
                if attr == "units":
                    units = var.getncattr(attr)
                elif attr == "positive" and var.getncattr(attr) in ["up","down","Up","Down"]:
                    positive = var.getncattr(attr)
                elif attr == "coordinates":
                    coordinates = var.getncattr(attr)
                elif attr == "_FillValue":
                    fill_value = var.getncattr(attr)
                elif attr == "missing_value":
                    missing_value = var.getncattr(attr)
                elif attr == "valid_min":
                    valid_min = var.getncattr(attr)
                elif attr == "valid_max":
                    valid_max = var.getncattr(attr)
                elif attr == "valid_range":
                    valid_range = var.getncattr(attr)
                elif attr == "add_offset":
                    add_offset = var.getncattr(attr)
                elif attr == "scale_factor":
                    scale_factor = var.getncattr(attr)
            cur.execute("INSERT INTO variable (dataset_id, name, datatype, num_dims, dims_names, dims_sizes, attrs) "
                        "VALUES ({}, \'{}\', \'{}\', {}, \'{}\', \'{}\', \'{}\') "
                        "RETURNING uid".
                        format(dataset_id, var.name, str(var.dtype), var.ndim, dims_names, dims_sizes,
                               json.dumps(attrs, cls=MyEncoder)))
            (var_id,) = cur.fetchone()

            values = var[:]
            # Init min/max because we compute min/max of the variable's values along the way.
            values_min = float("+inf")
            values_max = float("-inf")
            if var.ndim == 0:
                raise Exception("Variables depending on 0 dimensions are currently not supported!")
            elif var.ndim == 1:
                dim_0 = var.dimensions[0]
                coord_var_0 = get_coord_var(rootgrp, dim_0)
                if coord_var_0 is not None:
                    coord_var_0_values = coord_var_0[:]
                for (index_0,), value in np.ndenumerate(values):
                    if ((fill_value is not None and value == fill_value) or
                            (missing_value is not None and value == missing_value) or
                            (valid_min is not None and value < valid_min) or
                            (valid_max is not None and value > valid_max) or
                            (valid_range is not None and (value < valid_range[0] or value > valid_range[1]))):
                        value = "NULL"
                    else:
                        if scale_factor:
                            value *= scale_factor
                        if add_offset:
                            value += add_offset
                    value_0 = None
                    if coord_var_0 is not None:
                        value_0 = coord_var_0_values[index_0]
                    if batch_size_curr == batch_size_max-1:
                        batch += "({}, {}, {}, {})".format(var_id, index_0, value_0, value)
                        cur.execute("INSERT INTO value_1d (var_id, index_0, value_0, value) VALUES {}".format(batch))
                        batch = ""
                        batch_size_curr = 0
                    else:
                        batch += "({}, {}, {}, {}),".format(var_id, index_0, value_0, value)
                        batch_size_curr += 1
                    if value != "NULL":
                        values_min = min(values_min, value)
                        values_max = max(values_max, value)
                if batch:
                    cur.execute("INSERT INTO value_1d (var_id, index_0, value_0, value) VALUES {}".format(batch[:-1]))
                    batch = ""
                    batch_size_curr = 0
            elif var.ndim == 2:
                dim_0 = var.dimensions[0]
                dim_1 = var.dimensions[1]
                coord_var_0 = get_coord_var(rootgrp, dim_0)
                coord_var_1 = get_coord_var(rootgrp, dim_1)
                if coord_var_0 is not None:
                    coord_var_0_values = coord_var_0[:]
                if coord_var_1 is not None:
                    coord_var_1_values = coord_var_1[:]
                for (index_0, index_1), value in np.ndenumerate(values):
                    if ((fill_value is not None and value == fill_value) or
                            (missing_value is not None and value == missing_value) or
                            (valid_min is not None and value < valid_min) or
                            (valid_max is not None and value > valid_max) or
                            (valid_range is not None and (value < valid_range[0] or value > valid_range[1]))):
                        value = "NULL"
                    else:
                        if scale_factor:
                            value *= scale_factor
                        if add_offset:
                            value += add_offset
                    value_0 = None
                    value_1 = None
                    if coord_var_0 is not None:
                        value_0 = coord_var_0_values[index_0]
                    if coord_var_1 is not None:
                        value_1 = coord_var_1_values[index_1]
                    if batch_size_curr == batch_size_max-1:
                        batch += "({}, {}, {}, {}, {}, {})".format(var_id, index_0, value_0, index_1, value_1, value)
                        cur.execute("INSERT INTO value_2d (var_id, index_0, value_0, index_1, value_1, value) "
                                    "VALUES {}".format(batch))
                        batch = ""
                        batch_size_curr = 0
                    else:
                        batch += "({}, {}, {}, {}, {}, {}),".format(var_id, index_0, value_0, index_1, value_1, value)
                        batch_size_curr += 1
                    if value != "NULL":
                        values_min = min(values_min, value)
                        values_max = max(values_max, value)
                if batch:
                    cur.execute("INSERT INTO value_2d (var_id, index_0, value_0, index_1, value_1, value) "
                                "VALUES {}".format(batch[:-1]))
                    batch = ""
                    batch_size_curr = 0
            elif var.ndim == 3:
                dim_0 = var.dimensions[0]
                dim_1 = var.dimensions[1]
                dim_2 = var.dimensions[2]
                coord_var_0 = get_coord_var(rootgrp, dim_0)
                coord_var_1 = get_coord_var(rootgrp, dim_1)
                coord_var_2 = get_coord_var(rootgrp, dim_2)
                if coord_var_0 is not None:
                    coord_var_0_values = coord_var_0[:]
                if coord_var_1 is not None:
                    coord_var_1_values = coord_var_1[:]
                if coord_var_2 is not None:
                    coord_var_2_values = coord_var_2[:]
                for (index_0, index_1, index_2), value in np.ndenumerate(values):
                    if ((fill_value is not None and value == fill_value) or
                            (missing_value is not None and value == missing_value) or
                            (valid_min is not None and value < valid_min) or
                            (valid_max is not None and value > valid_max) or
                            (valid_range is not None and (value < valid_range[0] or value > valid_range[1]))):
                        value = "NULL"
                    else:
                        if scale_factor:
                            value *= scale_factor
                        if add_offset:
                            value += add_offset
                    value_0 = None
                    value_1 = None
                    value_2 = None
                    if coord_var_0 is not None:
                        value_0 = coord_var_0_values[index_0]
                    if coord_var_1 is not None:
                        value_1 = coord_var_1_values[index_1]
                    if coord_var_2 is not None:
                        value_2 = coord_var_2_values[index_2]
                    if batch_size_curr == batch_size_max-1:
                        batch += "({}, {}, {}, {}, {}, {}, {}, {})".format(var_id,
                                                                           index_0, value_0,
                                                                           index_1, value_1,
                                                                           index_2, value_2,
                                                                           value)
                        cur.execute("INSERT INTO value_3d (var_id, "
                                    "index_0, value_0, "
                                    "index_1, value_1, "
                                    "index_2, value_2, "
                                    "value) "
                                    "VALUES {}".format(batch))
                        batch = ""
                        batch_size_curr = 0
                    else:
                        batch += "({}, {}, {}, {}, {}, {}, {}, {}),".format(var_id,
                                                                    index_0, value_0,
                                                                    index_1, value_1,
                                                                    index_2, value_2,
                                                                    value)
                        batch_size_curr += 1
                    if value != "NULL":
                        values_min = min(values_min, value)
                        values_max = max(values_max, value)
                if batch:
                    cur.execute("INSERT INTO value_3d (var_id, "
                                "index_0, value_0, "
                                "index_1, value_1, "
                                "index_2, value_2, "
                                "value) "
                                "VALUES {}".format(batch[:-1]))
                    batch = ""
                    batch_size_curr = 0
            elif var.ndim == 4:
                dim_0 = var.dimensions[0]
                dim_1 = var.dimensions[1]
                dim_2 = var.dimensions[2]
                dim_3 = var.dimensions[3]
                coord_var_0 = get_coord_var(rootgrp, dim_0)
                coord_var_1 = get_coord_var(rootgrp, dim_1)
                coord_var_2 = get_coord_var(rootgrp, dim_2)
                coord_var_3 = get_coord_var(rootgrp, dim_3)
                if coord_var_0 is not None:
                    coord_var_0_values = coord_var_0[:]
                if coord_var_1 is not None:
                    coord_var_1_values = coord_var_1[:]
                if coord_var_2 is not None:
                    coord_var_2_values = coord_var_2[:]
                if coord_var_3 is not None:
                    coord_var_3_values = coord_var_3[:]
                for (index_0, index_1, index_2, index_3), value in np.ndenumerate(values):
                    if ((fill_value is not None and value == fill_value) or
                            (missing_value is not None and value == missing_value) or
                            (valid_min is not None and value < valid_min) or
                            (valid_max is not None and value > valid_max) or
                            (valid_range is not None and (value < valid_range[0] or value > valid_range[1]))):
                        value = "NULL"
                    else:
                        if scale_factor:
                            value *= scale_factor
                        if add_offset:
                            value += add_offset
                    value_0 = None
                    value_1 = None
                    value_2 = None
                    value_3 = None
                    if coord_var_0 is not None:
                        value_0 = coord_var_0_values[index_0]
                    if coord_var_1 is not None:
                        value_1 = coord_var_1_values[index_1]
                    if coord_var_2 is not None:
                        value_2 = coord_var_2_values[index_2]
                    if coord_var_3 is not None:
                        value_3 = coord_var_3_values[index_3]
                    if batch_size_curr == batch_size_max-1:
                        batch += "({}, {}, {}, {}, {}, {}, {}, {}, {}, {})".format(var_id,
                                                                   index_0, value_0,
                                                                   index_1, value_1,
                                                                   index_2, value_2,
                                                                   index_3, value_3,
                                                                   value)
                        cur.execute("INSERT INTO value_4d (var_id, "
                                    "index_0, value_0, "
                                    "index_1, value_1, "
                                    "index_2, value_2, "
                                    "index_3, value_3, "
                                    "value) "
                                    "VALUES {}".format(batch))
                        batch = ""
                        batch_size_curr = 0
                    else:
                        batch += "({}, {}, {}, {}, {}, {}, {}, {}, {}, {}),".format(var_id,
                                                                                   index_0, value_0,
                                                                                   index_1, value_1,
                                                                                   index_2, value_2,
                                                                                   index_3, value_3,
                                                                                   value)
                        batch_size_curr += 1
                    if value != "NULL":
                        values_min = min(values_min, value)
                        values_max = max(values_max, value)
                if batch:
                    cur.execute("INSERT INTO value_4d (var_id, "
                                "index_0, value_0, "
                                "index_1, value_1, "
                                "index_2, value_2, "
                                "index_3, value_3, "
                                "value) "
                                "VALUES {}".format(batch[:-1]))
                    batch = ""
                    batch_size_curr = 0
            else:
                raise Exception("Variables depending on > 4 dimensions are currently not supported!")

            # Now that we now the min/max of the variable update that info
            if values_min == float("+inf"):
                values_min = "NULL"
            if values_max == float("-inf"):
                values_max = "NULL"
            cur.execute("UPDATE variable SET min={}, max={} WHERE uid={}".
                        format(values_min, values_max, var_id))

            # Now comes the interesting part where we infer the variable's type,
            # i.e. whether it's T,Z,Y,X, or D (:= data variable), or unknown, i.e. NULL.
            # And along the way we also fill the corresponding other data table that
            # has a better understanding of the axes semantics.
            if units:
                if units in ["bar", "millibar", "decibar", "atmosphere", "atm", "pascal", "Pa", "hPa"] or positive:
                    var_type = "Z"
                    cur.execute("UPDATE variable SET type=\'{}\' WHERE uid={}".
                                format(var_type, var_id))
                elif units == "degrees_north":
                    var_type = "Y"
                    cur.execute("UPDATE variable SET type=\'{}\' WHERE uid={}".
                                format(var_type, var_id))
                elif units == "degrees_east":
                    var_type = "X"
                    cur.execute("UPDATE variable SET type=\'{}\' WHERE uid={}".
                                format(var_type, var_id))
                else:
                    if "since" in units:
                        units_comps = map(str.strip, map(str, units.split("since")))
                        if len(units_comps) == 2:
                            (time_units, time_ref) = units_comps
                            time_units_valid = time_units in ["d", "day", "days",
                                                              "h", "hr", "hour", "hrs", "hours",
                                                              "min", "minute", "mins", "minutes",
                                                              "s", "sec", "second", "secs", "seconds"]
                            time_ref_valid = validate_date(time_ref)
                            if time_units_valid and time_ref_valid:
                                var_type = "T"
                                cur.execute("UPDATE variable SET type=\'{}\' WHERE uid={}".
                                            format(var_type, var_id))
                    else:
                        # This is the most interesting case where it might be a data variable
                        # Here's where the clever inferring happens
                        if not coordinates:
                            # Simpler case where there is no :coordinates attribute
                            # In this case check if all dimensions have an associated coordinate variable
                            coord_vars = []
                            for dim in var.dimensions:
                                coord_vars.append(get_coord_var(rootgrp, dim))
                            if None in coord_vars:
                                pass
                            else:
                                # All dimensions have a corresponding variable, now need to check what kind of vars
                                coord_vars_types = map(get_coord_type_of_var, coord_vars)
                                if None in coord_vars_types:
                                    pass
                                else:
                                    # The corresponding variables are all coordinate variables!
                                    if coord_vars_types == ["T"]:
                                        [time_var] = coord_vars
                                        time_var_values = time_var[:]
                                        (time_interval, time_ref) = get_time_interval_and_ref(time_var)
                                        for (time_id,), value in np.ndenumerate(values):
                                            if ((fill_value is not None and value == fill_value) or
                                                    (missing_value is not None and value == missing_value) or
                                                    (valid_min is not None and value < valid_min) or
                                                    (valid_max is not None and value > valid_max) or
                                                    (valid_range is not None and (
                                                            value < valid_range[0] or value > valid_range[1]))):
                                                value = "NULL"
                                            else:
                                                if scale_factor:
                                                    value *= scale_factor
                                                if add_offset:
                                                    value += add_offset
                                            time_value = time_var_values[time_id]
                                            time_stamp = (time_ref +
                                                          datetime.timedelta(
                                                              seconds=time_value * time_interval.total_seconds()))
                                            time_stamp = time_stamp.strftime("%Y-%m-%d %H:%M:%S")
                                            if batch_size_curr == batch_size_max - 1:
                                                batch += "({}, {}, \'{}\', {})".format(var_id,
                                                                                       time_value, time_stamp,
                                                                                       value)
                                                cur.execute("INSERT INTO value_time "
                                                            "(var_id, time_value, time_stamp, value) "
                                                            "VALUES {}".format(batch))
                                                batch = ""
                                                batch_size_curr = 0
                                            else:
                                                batch += "({}, {}, \'{}\', {}),".format(var_id,
                                                                                       time_value, time_stamp,
                                                                                       value)
                                                batch_size_curr += 1
                                        if batch:
                                            cur.execute("INSERT INTO value_time "
                                                        "(var_id, time_value, time_stamp, value) "
                                                        "VALUES {}".format(batch[:-1]))
                                            batch = ""
                                            batch_size_curr = 0
                                    elif coord_vars_types == ["Z"]:
                                        [vertical_var] = coord_vars
                                        vertical_var_values = vertical_var[:]
                                        for (vertical_id,), value in np.ndenumerate(values):
                                            if ((fill_value is not None and value == fill_value) or
                                                    (missing_value is not None and value == missing_value) or
                                                    (valid_min is not None and value < valid_min) or
                                                    (valid_max is not None and value > valid_max) or
                                                    (valid_range is not None and (
                                                            value < valid_range[0] or value > valid_range[1]))):
                                                value = "NULL"
                                            else:
                                                if scale_factor:
                                                    value *= scale_factor
                                                if add_offset:
                                                    value += add_offset
                                            vertical_value = vertical_var_values[vertical_id]
                                            if batch_size_curr == batch_size_max - 1:
                                                batch += "({}, {}, {})".format(var_id, vertical_value, value)
                                                cur.execute("INSERT INTO value_vertical "
                                                            "(var_id, vertical_value, value) "
                                                            "VALUES {}".format(batch))
                                                batch = ""
                                                batch_size_curr = 0
                                            else:
                                                batch += "({}, {}, {}),".format(var_id, vertical_value, value)
                                                batch_size_curr += 1
                                        if batch:
                                            cur.execute("INSERT INTO value_vertical "
                                                        "(var_id, vertical_value, value) "
                                                        "VALUES {}".format(batch[:-1]))
                                            batch = ""
                                            batch_size_curr = 0
                                    elif coord_vars_types == ["Y", "X"]:
                                        [lat_var, lon_var] = coord_vars
                                        lat_var_values = lat_var[:]
                                        lon_var_values = lon_var[:]
                                        for (lat_id, lon_id), value in np.ndenumerate(values):
                                            if ((fill_value is not None and value == fill_value) or
                                                    (missing_value is not None and value == missing_value) or
                                                    (valid_min is not None and value < valid_min) or
                                                    (valid_max is not None and value > valid_max) or
                                                    (valid_range is not None and (
                                                            value < valid_range[0] or value > valid_range[1]))):
                                                value = "NULL"
                                            else:
                                                if scale_factor:
                                                    value *= scale_factor
                                                if add_offset:
                                                    value += add_offset
                                            lat_value = lat_var_values[lat_id]
                                            lon_value = lon_var_values[lon_id]
                                            geom = "ST_GeomFromText('POINT({} {})', 4326)".format(lon_value, lat_value)
                                            if batch_size_curr == batch_size_max - 1:
                                                batch += "({}, {}, {})".format(var_id, geom, value)
                                                cur.execute("INSERT INTO value_lat_lon "
                                                            "(var_id, geom, value) "
                                                            "VALUES {}".format(batch))
                                                batch = ""
                                                batch_size_curr = 0
                                            else:
                                                batch += "({}, {}, {}),".format(var_id, geom, value)
                                                batch_size_curr += 1
                                        if batch:
                                            cur.execute("INSERT INTO value_lat_lon "
                                                        "(var_id, geom, value) "
                                                        "VALUES {}".format(batch[:-1]))
                                            batch = ""
                                            batch_size_curr = 0
                                    elif coord_vars_types == ["T", "Y", "X"]:
                                        [time_var, lat_var, lon_var] = coord_vars
                                        time_var_values = time_var[:]
                                        lat_var_values = lat_var[:]
                                        lon_var_values = lon_var[:]
                                        (time_interval, time_ref) = get_time_interval_and_ref(time_var)
                                        for (time_id, lat_id, lon_id), value in np.ndenumerate(values):
                                            if ((fill_value is not None and value == fill_value) or
                                                    (missing_value is not None and value == missing_value) or
                                                    (valid_min is not None and value < valid_min) or
                                                    (valid_max is not None and value > valid_max) or
                                                    (valid_range is not None and (
                                                            value < valid_range[0] or value > valid_range[1]))):
                                                value = "NULL"
                                            else:
                                                if scale_factor:
                                                    value *= scale_factor
                                                if add_offset:
                                                    value += add_offset
                                            time_value = time_var_values[time_id]
                                            time_stamp = (time_ref +
                                                          datetime.timedelta(
                                                              seconds=time_value * time_interval.total_seconds()))
                                            time_stamp = time_stamp.strftime("%Y-%m-%d %H:%M:%S")
                                            lat_value = lat_var_values[lat_id]
                                            lon_value = lon_var_values[lon_id]
                                            geom = "ST_GeomFromText('POINT({} {})', 4326)".format(lon_value, lat_value)
                                            if batch_size_curr == batch_size_max - 1:
                                                batch += ("({}, {}, \'{}\', {}, {})".
                                                          format(var_id, time_value, time_stamp, geom, value))
                                                cur.execute("INSERT INTO value_time_lat_lon "
                                                            "(var_id, time_value, time_stamp, geom, value) "
                                                            "VALUES {}".format(batch))
                                                batch = ""
                                                batch_size_curr = 0
                                            else:
                                                batch += ("({}, {}, \'{}\', {}, {}),".
                                                          format(var_id, time_value, time_stamp, geom, value))
                                                batch_size_curr += 1
                                        if batch:
                                            cur.execute("INSERT INTO value_time_lat_lon "
                                                        "(var_id, time_value, time_stamp, geom, value) "
                                                        "VALUES {}".format(batch[:-1]))
                                            batch = ""
                                            batch_size_curr = 0
                                    elif coord_vars_types == ["Z", "Y", "X"]:
                                        [vertical_var, lat_var, lon_var] = coord_vars
                                        vertical_var_values = vertical_var[:]
                                        lat_var_values = lat_var[:]
                                        lon_var_values = lon_var[:]
                                        for (vertical_id, lat_id, lon_id), value in np.ndenumerate(values):
                                            if ((fill_value is not None and value == fill_value) or
                                                    (missing_value is not None and value == missing_value) or
                                                    (valid_min is not None and value < valid_min) or
                                                    (valid_max is not None and value > valid_max) or
                                                    (valid_range is not None and (
                                                            value < valid_range[0] or value > valid_range[1]))):
                                                value = "NULL"
                                            else:
                                                if scale_factor:
                                                    value *= scale_factor
                                                if add_offset:
                                                    value += add_offset
                                            vertical_value = vertical_var_values[vertical_id]
                                            lat_value = lat_var_values[lat_id]
                                            lon_value = lon_var_values[lon_id]
                                            geom = "ST_GeomFromText('POINT({} {})', 4326)".format(lon_value, lat_value)
                                            if batch_size_curr == batch_size_max - 1:
                                                batch += ("({}, {}, {}, {})".
                                                          format(var_id, vertical_value, geom, value))
                                                cur.execute("INSERT INTO value_vertical_lat_lon "
                                                            "(var_id, vertical_value, geom, value) "
                                                            "VALUES {}".format(batch))
                                                batch = ""
                                                batch_size_curr = 0
                                            else:
                                                batch += ("({}, {}, {}, {}),".
                                                          format(var_id, vertical_value, geom, value))
                                                batch_size_curr += 1
                                        if batch:
                                            cur.execute("INSERT INTO value_vertical_lat_lon "
                                                        "(var_id, vertical_value, geom, value) "
                                                        "VALUES {}".format(batch[:-1]))
                                            batch = ""
                                            batch_size_curr = 0
                                    elif coord_vars_types == ["T", "Z", "Y", "X"]:
                                        [time_var, vertical_var, lat_var, lon_var] = coord_vars
                                        time_var_values = time_var[:]
                                        vertical_var_values = vertical_var[:]
                                        lat_var_values = lat_var[:]
                                        lon_var_values = lon_var[:]
                                        (time_interval, time_ref) = get_time_interval_and_ref(time_var)
                                        for (time_id, vertical_id, lat_id, lon_id), value in np.ndenumerate(values):
                                            if ((fill_value is not None and value == fill_value) or
                                                    (missing_value is not None and value == missing_value) or
                                                    (valid_min is not None and value < valid_min) or
                                                    (valid_max is not None and value > valid_max) or
                                                    (valid_range is not None and (
                                                            value < valid_range[0] or value > valid_range[1]))):
                                                value = "NULL"
                                            else:
                                                if scale_factor:
                                                    value *= scale_factor
                                                if add_offset:
                                                    value += add_offset
                                            time_value = time_var_values[time_id]
                                            time_stamp = (time_ref +
                                                          datetime.timedelta(
                                                              seconds=time_value * time_interval.total_seconds()))
                                            time_stamp = time_stamp.strftime("%Y-%m-%d %H:%M:%S")
                                            vertical_value = vertical_var_values[vertical_id]
                                            lat_value = lat_var_values[lat_id]
                                            lon_value = lon_var_values[lon_id]
                                            geom = "ST_GeomFromText('POINT({} {})', 4326)".format(lon_value, lat_value)
                                            if batch_size_curr == batch_size_max - 1:
                                                batch += ("({}, {}, \'{}\', {}, {}, {})".
                                                          format(var_id,
                                                                 time_value, time_stamp,
                                                                 vertical_value, geom, value))
                                                cur.execute("INSERT INTO value_time_vertical_lat_lon "
                                                            "(var_id, "
                                                            "time_value, time_stamp, "
                                                            "vertical_value, geom, value) "
                                                            "VALUES {}".format(batch))
                                                batch = ""
                                                batch_size_curr = 0
                                            else:
                                                batch += ("({}, {}, \'{}\', {}, {}, {}),".
                                                          format(var_id,
                                                                 time_value, time_stamp,
                                                                 vertical_value, geom, value))
                                                batch_size_curr += 1
                                        if batch:
                                            cur.execute("INSERT INTO value_time_vertical_lat_lon "
                                                        "(var_id, "
                                                        "time_value, time_stamp, "
                                                        "vertical_value, geom, value) "
                                                        "VALUES {}".format(batch[:-1]))
                                            batch = ""
                                            batch_size_curr = 0
                                    else:
                                        raise Exception("For data variables only the cases T;Z;Y,X;T,Y,X;Z,Y,X;T,Z,Y,X"
                                                        "are currently supported!")
                                    var_type = "D"
                                    axes = '{' + ','.join(['\"{}\"'.format(t) for t in coord_vars_types]) + '}'
                                    axes_mins = '{' + ','.join([str(np.amin(cvar[:])) for cvar in coord_vars]) + '}'
                                    axes_maxs = '{' + ','.join([str(np.amax(cvar[:])) for cvar in coord_vars]) + '}'
                                    axes_units = ('{' +
                                                  ','.join(['\"{}\"'.format(cvar.getncattr("units"))
                                                            for cvar in coord_vars])
                                                  + '}')
                                    cur.execute("UPDATE variable "
                                                "SET type=\'{}\', axes=\'{}\', axes_mins=\'{}\', axes_maxs=\'{}\', "
                                                "axes_units=\'{}\' "
                                                "WHERE uid={}".
                                                format(var_type, axes, axes_mins, axes_maxs, axes_units, var_id))
                        else:
                            # Complex case where there is a :coordinates attribute
                            # We first gather all the variables we can infer, i.e. coordinate variables and auxiliary.
                            # For every inferred variable we store a list of indexes that stores which dimensions will
                            # be plugged in when computing the axis values.
                            # First we gather the coordinate variables, i.e. variables that have the same name as
                            # a dimension and only depend on that dimension.
                            inferred_vars = []
                            for index, dim in enumerate(var.dimensions):
                                inferred_vars.append((get_coord_var(rootgrp, dim), [index]))
                            # Next, let's gather the variables that can be obtained through the :coordinates attribute
                            auxiliary_vars_names = map(str.strip, map(str, coordinates.split(' ')))
                            for aux_var_name in auxiliary_vars_names:
                                # If :coordinates contains a variable name that doesn't occur this will throw an Exception
                                aux_var = rootgrp.variables[aux_var_name]
                                indexes = []
                                for dim in aux_var.dimensions:
                                    index = var.dimensions.index(dim)
                                    indexes.append(index)
                                inferred_vars.append((aux_var, indexes))
                            # Determine which are coordinate variables and if so what kind
                            coordinate_vars_types = map(get_coord_type_of_var, [ivar[0] for ivar in inferred_vars])
                            # Order the T,Z,Y,X coordinate variables we were able to find
                            coordinate_vars_sorted = []
                            coordinate_vars_type_sorted = []
                            try:
                                index = coordinate_vars_types.index("T")
                                coordinate_vars_sorted.append(inferred_vars[index])
                                coordinate_vars_type_sorted.append("T")
                            except ValueError:
                                pass
                            try:
                                index = coordinate_vars_types.index("Z")
                                coordinate_vars_sorted.append(inferred_vars[index])
                                coordinate_vars_type_sorted.append("Z")
                            except ValueError:
                                pass
                            try:
                                index = coordinate_vars_types.index("Y")
                                coordinate_vars_sorted.append(inferred_vars[index])
                                coordinate_vars_type_sorted.append("Y")
                            except ValueError:
                                pass
                            try:
                                index = coordinate_vars_types.index("X")
                                coordinate_vars_sorted.append(inferred_vars[index])
                                coordinate_vars_type_sorted.append("X")
                            except ValueError:
                                pass
                            # Do the case distinction based on T,Z,Y,X
                            if coordinate_vars_type_sorted == ["T"]:
                                (time_var, time_deps) = coordinate_vars_sorted[0]
                                time_var_values = time_var[:]
                                (time_interval, time_ref) = get_time_interval_and_ref(time_var)
                                for index, value in np.ndenumerate(values):
                                    if ((fill_value is not None and value == fill_value) or
                                            (missing_value is not None and value == missing_value) or
                                            (valid_min is not None and value < valid_min) or
                                            (valid_max is not None and value > valid_max) or
                                            (valid_range is not None and (
                                                    value < valid_range[0] or value > valid_range[1]))):
                                        value = "NULL"
                                    else:
                                        if scale_factor:
                                            value *= scale_factor
                                        if add_offset:
                                            value += add_offset
                                    time_index = tuple([index[i] for i in time_deps])
                                    time_value = time_var_values[time_index]
                                    time_stamp = (time_ref +
                                                  datetime.timedelta(
                                                      seconds=time_value * time_interval.total_seconds()))
                                    time_stamp = time_stamp.strftime("%Y-%m-%d %H:%M:%S")
                                    if batch_size_curr == batch_size_max - 1:
                                        batch += "({}, {}, \'{}\', {})".format(var_id,
                                                                               time_value, time_stamp,
                                                                               value)
                                        cur.execute("INSERT INTO value_time "
                                                    "(var_id, time_value, time_stamp, value) "
                                                    "VALUES {}".format(batch))
                                        batch = ""
                                        batch_size_curr = 0
                                    else:
                                        batch += "({}, {}, \'{}\', {}),".format(var_id,
                                                                                time_value, time_stamp,
                                                                                value)
                                        batch_size_curr += 1
                                if batch:
                                    cur.execute("INSERT INTO value_time "
                                                "(var_id, time_value, time_stamp, value) "
                                                "VALUES {}".format(batch[:-1]))
                                    batch = ""
                                    batch_size_curr = 0
                            elif coordinate_vars_type_sorted == ["Z"]:
                                (vertical_var, vertical_deps) = coordinate_vars_sorted[0]
                                vertical_var_values = vertical_var[:]
                                for index, value in np.ndenumerate(values):
                                    if ((fill_value is not None and value == fill_value) or
                                            (missing_value is not None and value == missing_value) or
                                            (valid_min is not None and value < valid_min) or
                                            (valid_max is not None and value > valid_max) or
                                            (valid_range is not None and (
                                                    value < valid_range[0] or value > valid_range[1]))):
                                        value = "NULL"
                                    else:
                                        if scale_factor:
                                            value *= scale_factor
                                        if add_offset:
                                            value += add_offset
                                    vertical_index = tuple([index[i] for i in vertical_deps])
                                    vertical_value = vertical_var_values[vertical_index]
                                    if batch_size_curr == batch_size_max - 1:
                                        batch += "({}, {}, {})".format(var_id, vertical_value, value)
                                        cur.execute("INSERT INTO value_vertical "
                                                    "(var_id, vertical_value, value) "
                                                    "VALUES {}".format(batch))
                                        batch = ""
                                        batch_size_curr = 0
                                    else:
                                        batch += "({}, {}, {}),".format(var_id, vertical_value, value)
                                        batch_size_curr += 1
                                if batch:
                                    cur.execute("INSERT INTO value_vertical "
                                                "(var_id, vertical_value, value) "
                                                "VALUES {}".format(batch[:-1]))
                                    batch = ""
                                    batch_size_curr = 0
                            elif coordinate_vars_type_sorted == ["Y", "X"]:
                                (lat_var, lat_deps) = coordinate_vars_sorted[0]
                                (lon_var, lon_deps) = coordinate_vars_sorted[1]
                                lat_var_values = lat_var[:]
                                lon_var_values = lon_var[:]
                                for index, value in np.ndenumerate(values):
                                    if ((fill_value is not None and value == fill_value) or
                                            (missing_value is not None and value == missing_value) or
                                            (valid_min is not None and value < valid_min) or
                                            (valid_max is not None and value > valid_max) or
                                            (valid_range is not None and (
                                                    value < valid_range[0] or value > valid_range[1]))):
                                        value = "NULL"
                                    else:
                                        if scale_factor:
                                            value *= scale_factor
                                        if add_offset:
                                            value += add_offset
                                    lat_index = tuple([index[i] for i in lat_deps])
                                    lon_index = tuple([index[i] for i in lon_deps])
                                    lat_value = lat_var_values[lat_index]
                                    lon_value = lon_var_values[lon_index]
                                    geom = "ST_GeomFromText('POINT({} {})', 4326)".format(lon_value, lat_value)
                                    if batch_size_curr == batch_size_max - 1:
                                        batch += "({}, {}, {})".format(var_id, geom, value)
                                        cur.execute("INSERT INTO value_lat_lon "
                                                    "(var_id, geom, value) "
                                                    "VALUES {}".format(batch))
                                        batch = ""
                                        batch_size_curr = 0
                                    else:
                                        batch += "({}, {}, {}),".format(var_id, geom, value)
                                        batch_size_curr += 1
                                if batch:
                                    cur.execute("INSERT INTO value_lat_lon "
                                                "(var_id, geom, value) "
                                                "VALUES {}".format(batch[:-1]))
                                    batch = ""
                                    batch_size_curr = 0
                            elif coordinate_vars_type_sorted == ["T", "Y", "X"]:
                                (time_var, time_deps) = coordinate_vars_sorted[0]
                                (lat_var, lat_deps) = coordinate_vars_sorted[1]
                                (lon_var, lon_deps) = coordinate_vars_sorted[2]
                                time_var_values = time_var[:]
                                lat_var_values = lat_var[:]
                                lon_var_values = lon_var[:]
                                (time_interval, time_ref) = get_time_interval_and_ref(time_var)
                                for index, value in np.ndenumerate(values):
                                    if ((fill_value is not None and value == fill_value) or
                                            (missing_value is not None and value == missing_value) or
                                            (valid_min is not None and value < valid_min) or
                                            (valid_max is not None and value > valid_max) or
                                            (valid_range is not None and (
                                                    value < valid_range[0] or value > valid_range[1]))):
                                        value = "NULL"
                                    else:
                                        if scale_factor:
                                            value *= scale_factor
                                        if add_offset:
                                            value += add_offset
                                    time_index = tuple([index[i] for i in time_deps])
                                    time_value = time_var_values[time_index]
                                    time_stamp = (time_ref +
                                                  datetime.timedelta(
                                                      seconds=time_value * time_interval.total_seconds()))
                                    time_stamp = time_stamp.strftime("%Y-%m-%d %H:%M:%S")
                                    lat_index = tuple([index[i] for i in lat_deps])
                                    lon_index = tuple([index[i] for i in lon_deps])
                                    lat_value = lat_var_values[lat_index]
                                    lon_value = lon_var_values[lon_index]
                                    geom = "ST_GeomFromText('POINT({} {})', 4326)".format(lon_value, lat_value)
                                    if batch_size_curr == batch_size_max - 1:
                                        batch += ("({}, {}, \'{}\', {}, {})".
                                                  format(var_id, time_value, time_stamp, geom, value))
                                        cur.execute("INSERT INTO value_time_lat_lon "
                                                    "(var_id, time_value, time_stamp, geom, value) "
                                                    "VALUES {}".format(batch))
                                        batch = ""
                                        batch_size_curr = 0
                                    else:
                                        batch += ("({}, {}, \'{}\', {}, {}),".
                                                  format(var_id, time_value, time_stamp, geom, value))
                                        batch_size_curr += 1
                                if batch:
                                    cur.execute("INSERT INTO value_time_lat_lon "
                                                "(var_id, time_value, time_stamp, geom, value) "
                                                "VALUES {}".format(batch[:-1]))
                                    batch = ""
                                    batch_size_curr = 0
                            elif coordinate_vars_type_sorted == ["Z", "Y", "X"]:
                                (vertical_var, vertical_deps) = coordinate_vars_sorted[0]
                                (lat_var, lat_deps) = coordinate_vars_sorted[1]
                                (lon_var, lon_deps) = coordinate_vars_sorted[2]
                                vertical_var_values = vertical_var[:]
                                lat_var_values = lat_var[:]
                                lon_var_values = lon_var[:]
                                for index, value in np.ndenumerate(values):
                                    if ((fill_value is not None and value == fill_value) or
                                            (missing_value is not None and value == missing_value) or
                                            (valid_min is not None and value < valid_min) or
                                            (valid_max is not None and value > valid_max) or
                                            (valid_range is not None and (
                                                    value < valid_range[0] or value > valid_range[1]))):
                                        value = "NULL"
                                    else:
                                        if scale_factor:
                                            value *= scale_factor
                                        if add_offset:
                                            value += add_offset
                                    vertical_index = tuple([index[i] for i in vertical_deps])
                                    vertical_value = vertical_var_values[vertical_index]
                                    lat_index = tuple([index[i] for i in lat_deps])
                                    lon_index = tuple([index[i] for i in lon_deps])
                                    lat_value = lat_var_values[lat_index]
                                    lon_value = lon_var_values[lon_index]
                                    geom = "ST_GeomFromText('POINT({} {})', 4326)".format(lon_value, lat_value)
                                    if batch_size_curr == batch_size_max - 1:
                                        batch += ("({}, {}, {}, {})".
                                                  format(var_id, vertical_value, geom, value))
                                        cur.execute("INSERT INTO value_vertical_lat_lon "
                                                    "(var_id, vertical_value, geom, value) "
                                                    "VALUES {}".format(batch))
                                        batch = ""
                                        batch_size_curr = 0
                                    else:
                                        batch += ("({}, {}, {}, {}),".
                                                  format(var_id, vertical_value, geom, value))
                                        batch_size_curr += 1
                                if batch:
                                    cur.execute("INSERT INTO value_vertical_lat_lon "
                                                "(var_id, vertical_value, geom, value) "
                                                "VALUES {}".format(batch[:-1]))
                                    batch = ""
                                    batch_size_curr = 0
                            elif coordinate_vars_type_sorted == ["T", "Z", "Y", "X"]:
                                (time_var, time_deps) = coordinate_vars_sorted[0]
                                (vertical_var, vertical_deps) = coordinate_vars_sorted[1]
                                (lat_var, lat_deps) = coordinate_vars_sorted[2]
                                (lon_var, lon_deps) = coordinate_vars_sorted[3]
                                time_var_values = time_var[:]
                                vertical_var_values = vertical_var[:]
                                lat_var_values = lat_var[:]
                                lon_var_values = lon_var[:]
                                (time_interval, time_ref) = get_time_interval_and_ref(time_var)
                                for index, value in np.ndenumerate(values):
                                    if ((fill_value is not None and value == fill_value) or
                                            (missing_value is not None and value == missing_value) or
                                            (valid_min is not None and value < valid_min) or
                                            (valid_max is not None and value > valid_max) or
                                            (valid_range is not None and (
                                                    value < valid_range[0] or value > valid_range[1]))):
                                        value = "NULL"
                                    else:
                                        if scale_factor:
                                            value *= scale_factor
                                        if add_offset:
                                            value += add_offset
                                    time_index = tuple([index[i] for i in time_deps])
                                    time_value = time_var_values[time_index]
                                    time_stamp = (time_ref +
                                                  datetime.timedelta(
                                                      seconds=time_value * time_interval.total_seconds()))
                                    time_stamp = time_stamp.strftime("%Y-%m-%d %H:%M:%S")
                                    vertical_index = tuple([index[i] for i in vertical_deps])
                                    vertical_value = vertical_var_values[vertical_index]
                                    lat_index = tuple([index[i] for i in lat_deps])
                                    lon_index = tuple([index[i] for i in lon_deps])
                                    lat_value = lat_var_values[lat_index]
                                    lon_value = lon_var_values[lon_index]
                                    geom = "ST_GeomFromText('POINT({} {})', 4326)".format(lon_value, lat_value)
                                    if batch_size_curr == batch_size_max - 1:
                                        batch += ("({}, {}, \'{}\', {}, {}, {})".
                                                  format(var_id,
                                                         time_value, time_stamp,
                                                         vertical_value, geom, value))
                                        cur.execute("INSERT INTO value_time_vertical_lat_lon "
                                                    "(var_id, "
                                                    "time_value, time_stamp, "
                                                    "vertical_value, geom, value) "
                                                    "VALUES {}".format(batch))
                                        batch = ""
                                        batch_size_curr = 0
                                    else:
                                        batch += ("({}, {}, \'{}\', {}, {}, {}),".
                                                  format(var_id,
                                                         time_value, time_stamp,
                                                         vertical_value, geom, value))
                                        batch_size_curr += 1
                                if batch:
                                    cur.execute("INSERT INTO value_time_vertical_lat_lon "
                                                "(var_id, "
                                                "time_value, time_stamp, "
                                                "vertical_value, geom, value) "
                                                "VALUES {}".format(batch[:-1]))
                                    batch = ""
                                    batch_size_curr = 0
                            else:
                                raise Exception("Variable {} has the :coordinates attribute but it does not lead to"
                                                "either of the cases T;Z;Y,X;Z,Y,X;T,Y,X;T,Z,Y,X".
                                                format(var.name))
                            var_type = "D"
                            coordinate_vars_sorted_without_deps = [cvar[0] for cvar in coordinate_vars_sorted]
                            axes = '{' + ','.join(['\"{}\"'.format(t) for t in coordinate_vars_type_sorted]) + '}'
                            axes_mins = '{' + ','.join([str(np.amin(cvar[:]))
                                                        for cvar in coordinate_vars_sorted_without_deps]) + '}'
                            axes_maxs = '{' + ','.join([str(np.amax(cvar[:]))
                                                        for cvar in coordinate_vars_sorted_without_deps]) + '}'
                            axes_units = ('{' +
                                          ','.join(['\"{}\"'.format(cvar.getncattr("units"))
                                                    for cvar in coordinate_vars_sorted_without_deps])
                                          + '}')
                            cur.execute("UPDATE variable "
                                        "SET type=\'{}\', axes=\'{}\', axes_mins=\'{}\', axes_maxs=\'{}\', "
                                        "axes_units=\'{}\' "
                                        "WHERE uid={}".
                                        format(var_type, axes, axes_mins, axes_maxs, axes_units, var_id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        raise Exception("ingest_netcdf: Could not ingest NetCDF: {}".format(filename))
    finally:
        conn.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Arguments for ingesting NetCDF')
    parser.add_argument('--input', help='Input NetCDF', required=True)
    args = parser.parse_args()
    try:
        ingest_netcdf(args.input)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        print("main: Could not ingest NetCDF: {}".format(args.input))
        sys.exit()