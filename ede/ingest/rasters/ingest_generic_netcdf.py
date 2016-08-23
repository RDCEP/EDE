import argparse
import sys
from netCDF4 import Dataset
import psycopg2
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST
import json
import numpy as np
import datetime


def validate_date(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD HH:MM:SS")


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
    else:
        if "since" in units:
            units_comps = map(str.strip, units.split("since"))
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


def get_time_interval_and_ref(time_var):
    units = time_var.getncattr("units")
    (time_units, time_ref) = map(str.strip, units.split("since"))
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


def ingest_netcdf(filename):

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
                    format(attrs))
        (dataset_id,) = cur.fetchone()

        for var in rootgrp.variables.values():
            dims_names = '{' + '\",\"'.join(var.dimensions) + '}'
            dims_sizes = '{' + '\",\"'.join(var.shape) + '}'
            attrs = []
            units = None
            positive = None
            coordinates = None
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
            cur.execute("INSERT INTO variable (dataset_id, name, datatype, num_dims, dims_names, dims_sizes, attrs) "
                        "VALUES ({}, \'{}\', \'{}\', {}, \'{}\', \'{}\', \'{}\') "
                        "RETURNING uid".
                        format(dataset_id, var.name, str(var.dtype), var.ndim, dims_names, dims_sizes,
                               json.dumps(attrs)))
            (var_id,) = cur.fetchone()

            values = var[:]
            # Init min/max because we compute min/max of the variable's values along the way.
            values_min = float("+inf")
            values_max = float("-inf")
            if var.ndim == 0:
                raise Exception("Variables depending on 0 dimensions are currently not supported!")
            elif var.ndim == 1:
                dim_0 = var.dimensions[0]
                coord_var_0 = get_coord_var(dim_0)
                for (index_0), value in np.ndenumerate(values):
                    value_0 = None
                    if coord_var_0:
                        value_0 = coord_var_0[index_0]
                    cur.execute("INSERT INTO value_1d (var_id, index_0, value_0, value) "
                                "VALUES ({}, {}, {}, {})".
                                format(var_id, index_0, value_0, value))
                    values_min = min(values_min, value)
                    values_max = min(values_max, value)
            elif var.ndim == 2:
                dim_0 = var.dimensions[0]
                dim_1 = var.dimensions[1]
                coord_var_0 = get_coord_var(dim_0)
                coord_var_1 = get_coord_var(dim_1)
                for (index_0, index_1), value in np.ndenumerate(values):
                    value_0 = None
                    value_1 = None
                    if coord_var_0:
                        value_0 = coord_var_0[index_0]
                    if coord_var_1:
                        value_1 = coord_var_1[index_1]
                    cur.execute("INSERT INTO value_2d (var_id, index_0, value_0, index_1, value_1, value) "
                                "VALUES ({}, {}, {}, {}, {}, {})".
                                format(var_id, index_0, value_0, index_1, value_1, value))
                    values_min = min(values_min, value)
                    values_max = min(values_max, value)
            elif var.ndim == 3:
                dim_0 = var.dimensions[0]
                dim_1 = var.dimensions[1]
                dim_2 = var.dimensions[2]
                coord_var_0 = get_coord_var(dim_0)
                coord_var_1 = get_coord_var(dim_1)
                coord_var_2 = get_coord_var(dim_2)
                for (index_0, index_1, index_2), value in np.ndenumerate(values):
                    value_0 = None
                    value_1 = None
                    value_2 = None
                    if coord_var_0:
                        value_0 = coord_var_0[index_0]
                    if coord_var_1:
                        value_1 = coord_var_1[index_1]
                    if coord_var_2:
                        value_2 = coord_var_2[index_2]
                    cur.execute("INSERT INTO value_3d (var_id, index_0, value_0, index_1, value_1, "
                                "index_2, value_2, value) "
                                "VALUES ({}, {}, {}, {}, {}, {}, {}, {})".
                                format(var_id, index_0, value_0, index_1, value_1, index_2, value_2, value))
                    values_min = min(values_min, value)
                    values_max = min(values_max, value)
            elif var.ndim == 4:
                dim_0 = var.dimensions[0]
                dim_1 = var.dimensions[1]
                dim_2 = var.dimensions[2]
                dim_3 = var.dimensions[3]
                coord_var_0 = get_coord_var(dim_0)
                coord_var_1 = get_coord_var(dim_1)
                coord_var_2 = get_coord_var(dim_2)
                coord_var_3 = get_coord_var(dim_3)
                for (index_0, index_1, index_2, index_3), value in np.ndenumerate(values):
                    value_0 = None
                    value_1 = None
                    value_2 = None
                    value_3 = None
                    if coord_var_0:
                        value_0 = coord_var_0[index_0]
                    if coord_var_1:
                        value_1 = coord_var_1[index_1]
                    if coord_var_2:
                        value_2 = coord_var_2[index_2]
                    if coord_var_3:
                        value_3 = coord_var_3[index_3]
                    cur.execute("INSERT INTO value_3d (var_id, index_0, value_0, index_1, value_1, "
                                "index_2, value_2, index_3, value_3, value) "
                                "VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})".
                                format(var_id, index_0, value_0, index_1, value_1, index_2, value_2,
                                       index_3, value_3, value))
                    values_min = min(values_min, value)
                    values_max = min(values_max, value)

                # Now that we now the min/max of the variable update that info
                cur.execute("UPDATE variable SET min={}, max={} WHERE uid={}".
                            format(values_min, values_max, var_id))

                # Now comes the interesting part where we infer the variable's type,
                # i.e. whether it's T,Z,Y,X, or D (:= data variable), or unknown, i.e. NULL.
                # And along the way we also fill the corresponding other data table that
                # has a better understanding of the axes semantics.
                if units in ["bar", "millibar", "decibar", "atmosphere", "atm", "pascal", "Pa", "hPa"] or positive:
                    var_type = "Z"
                    cur.execute("UPDATE variable SET type=\"{}\" WHERE uid={}".
                                format(var_type, var_id))
                elif units == "degrees_north":
                    var_type = "Y"
                    cur.execute("UPDATE variable SET type=\"{}\" WHERE uid={}".
                                format(var_type, var_id))
                elif units == "degrees_east":
                    var_type = "X"
                    cur.execute("UPDATE variable SET type=\"{}\" WHERE uid={}".
                                format(var_type, var_id))
                else:
                    if "since" in units:
                        units_comps = map(str.strip, units.split("since"))
                        if len(units_comps) == 2:
                            (time_units, time_ref) = units_comps
                            time_units_valid = time_units in ["d", "day", "days",
                                                              "h", "hr", "hour", "hrs", "hours",
                                                              "min", "minute", "mins", "minutes",
                                                              "s", "sec", "second", "secs", "seconds"]
                            time_ref_valid = validate_date(time_ref)
                            if time_units_valid and time_ref_valid:
                                var_type = "T"
                                cur.execute("UPDATE variable SET type=\"{}\" WHERE uid={}".
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
                                        (time_var) = coord_vars
                                        (time_interval, time_ref) = get_time_interval_and_ref(time_var)
                                        for (time_id, value) in np.ndenumerate(values):
                                            time_value = time_var[time_id]
                                            time_stamp = time_ref + time_value * time_interval
                                            time_stamp = time_stamp.strftime("%Y-%m-%d %H:%M:%S")
                                            cur.execute("INSERT INTO value_time "
                                                        "(var_id, time_value, time_stamp, value) "
                                                        "VALUES ({}, {}, \'{}\', {})".
                                                        format(var_id, time_value, time_stamp, value))
                                    elif coord_vars_types == ["Z"]:
                                        (vertical_var) = coord_vars
                                        for (vertical_id), value in np.ndenumerate(values):
                                            vertical_value = vertical_var[vertical_id]
                                            cur.execute("INSERT INTO value_vertical "
                                                        "(var_id, vertical_value, value) "
                                                        "VALUES ({}, {}, {})".
                                                        format(var_id, vertical_value, value))
                                    elif coord_vars_types == ["Y", "X"]:
                                        (lat_var, lon_var) = coord_vars
                                        for (lat_id, lon_id), value in np.ndenumerate(values):
                                            lat_value = lat_var[lat_id]
                                            lon_value = lon_var[lon_id]
                                            geom = "ST_GeomFromText('POINT({} {})', 4326)".format(lon_value, lat_value)
                                            cur.execute("INSERT INTO value_lat_lon "
                                                        "(var_id, geom, value) "
                                                        "VALUES ({}, {}, {})".
                                                        format(var_id, geom, value))
                                    elif coord_vars_types == ["T", "Y", "X"]:
                                        (time_var, lat_var, lon_var) = coord_vars
                                        (time_interval, time_ref) = get_time_interval_and_ref(time_var)
                                        for (time_id, lat_id, lon_id), value in np.ndenumerate(values):
                                            time_value = time_var[time_id]
                                            time_stamp = time_ref + time_value * time_interval
                                            time_stamp = time_stamp.strftime("%Y-%m-%d %H:%M:%S")
                                            lat_value = lat_var[lat_id]
                                            lon_value = lon_var[lon_id]
                                            geom = "ST_GeomFromText('POINT({} {})', 4326)".format(lon_value, lat_value)
                                            cur.execute("INSERT INTO value_time_lat_lon "
                                                        "(var_id, time_value, time_stamp, geom, value) "
                                                        "VALUES ({}, {}, \'{}\', {}, {})".
                                                        format(var_id, time_value, time_stamp, geom, value))
                                    elif coord_vars_types == ["Z", "Y", "X"]:
                                        (vertical_var, lat_var, lon_var) = coord_vars
                                        for (vertical_id, lat_id, lon_id), value in np.ndenumerate(values):
                                            vertical_value = vertical_var[vertical_id]
                                            lat_value = lat_var[lat_id]
                                            lon_value = lon_var[lon_id]
                                            geom = "ST_GeomFromText('POINT({} {})', 4326)".format(lon_value, lat_value)
                                            cur.execute("INSERT INTO value_vertical_lat_lon "
                                                        "(var_id, vertical_value, geom, value) "
                                                        "VALUES ({}, {}, {}, {})".
                                                        format(var_id, vertical_value, geom, value))
                                    elif coord_vars_types == ["T", "Z", "Y", "X"]:
                                        (time_var, vertical_var, lat_var, lon_var) in coord_vars
                                        (time_interval, time_ref) = get_time_interval_and_ref(time_var)
                                        for (time_id, vertical_id, lat_id, lon_id), value in np.ndenumerate(values):
                                            time_value = time_var[time_id]
                                            time_stamp = time_ref + time_value * time_interval
                                            time_stamp = time_stamp.strftime("%Y-%m-%d %H:%M:%S")
                                            vertical_value = vertical_var[vertical_id]
                                            lat_value = lat_var[lat_id]
                                            lon_value = lon_var[lon_id]
                                            geom = "ST_GeomFromText('POINT({} {})', 4326)".format(lon_value, lat_value)
                                            cur.execute("INSERT INTO value_time_vertical_lat_lon "
                                                        "(var_id, time_value, time_stamp, vertical_value, geom, value) "
                                                        "VALUES ({}, {}, \'{}\', {}, {}, {})".
                                                        format(var_id, time_value, time_stamp, vertical_value,
                                                               geom, value))
                                    else:
                                        raise Exception("For data variables only the cases T;Z;Y,X;T,Y,X;Z,Y,X;T,Z,Y,X"
                                                        "are currently supported!")
                                    var_type = "D"
                                    axes = '{' + '\",\"'.join(coord_vars_types) + '}'
                                    axes_mins = [np.amin(cvar[:]) for cvar in coord_vars]
                                    axes_maxs = [np.amax(cvar[:]) for cvar in coord_vars]
                                    axes_units = [cvar.getncattr("units") for cvar in coord_vars]
                                    cur.execute("UPDATE variable "
                                                "SET type=\"{}\", axes=\'{}\', axes_mins=\'{}\', axes_maxs=\'{}\', "
                                                "axes_units=\'{}\' "
                                                "WHERE uid={}".
                                                format(var_type, axes, axes_mins, axes_maxs, axes_units, var_id))
                        else:
                            # Complex case where there is a :coordinates attribute
                            # TODO
                            raise Exception("Variables with a :coordinates attribute cannot yet be handled!")
            else:
                raise Exception("Variables depending on more than 4 dimensions are currently not supported!")

        conn.commit()
    except:
        conn.rollback()
    finally:
        conn.close()


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