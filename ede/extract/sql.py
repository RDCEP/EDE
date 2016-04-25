#!/usr/bin/env python
# -*- coding: utf-8 -*-


GRID_META = """SELECT uid, filename, filesize, filetype, meta_data,
date_created, date_inserted
FROM grid_meta;
"""

GRID_META_BY_UID = """SELECT uid, filename, filesize, filetype, meta_data,
date_created, date_inserted
FROM grid_meta
WHERE uid in {};
"""

MAKE_POLY = """ST_Polygon(ST_GeomFromText(
'LINESTRING({} {}, {} {}, {} {}, {} {}, {} {})'), 4326)
"""

GRID_DATA = """SELECT ST_X(geom), ST_Y(geom), val
FROM (SELECT(ST_PixelAsCentroids(ST_Clip(rast, {}, TRUE))).*
FROM grid_data AS gd
WHERE meta_id={} AND var_id={}) foo;
"""
GRID_DATA_BY_DATE = """SELECT ST_X(geom), ST_Y(geom), val
FROM (SELECT(ST_PixelAsCentroids(ST_Clip(rast, {}, TRUE))).*
FROM grid_data AS gd
WHERE meta_id={} AND var_id={} AND date={})
foo;
"""
GRID_DATA_BY_DATE_AND_POLYID = """SELECT ST_X(geom), ST_Y(geom), val
FROM (SELECT(ST_PixelAsCentroids(ST_Clip(rast, {}, TRUE))).*
FROM grid_data AS gd, regions AS r
WHERE gd.meta_id={} AND gd.var_id={} AND r.uid={} AND gd.date={})
foo;
"""
GRID_DATA_BY_POLYID = """SELECT ST_X(geom), ST_Y(geom), val
FROM (SELECT(ST_PixelAsCentroids(ST_Clip(rast, {}, TRUE))).*
FROM grid_data AS gd, regions AS r
WHERE gd.meta_id={} AND gd.var_id={} AND r.uid={})
foo;
"""

DATE = """SELECT to_char(DATE, \'YYYY-MM-DD HH24:MI:SS\')
FROM grid_dates;
"""
DATE_BY_ID = """SELECT to_char(DATE, \'YYYY-MM-DD HH24:MI:SS\')
FROM grid_dates
WHERE uid={};
"""

SPATIAL_AGG = """SELECT ST_SummaryStats(ST_Union(ST_Clip(rast, {}, TRUE)))
FROM grid_data
WHERE meta_id={} AND var_id={};
"""

SPATIAL_AGG_BY_DATE = """SELECT ST_SummaryStats(ST_Union(ST_Clip(rast, {}, TRUE)))
FROM grid_data
WHERE meta_id={} AND var_id={} AND date={};
"""

SPATIAL_AGG_BY_POLYID = """SELECT ST_SummaryStats(ST_Union(ST_Clip(rast, r.geom, TRUE)))
FROM grid_data AS gd, regions AS r
WHERE gd.meta_id={} AND gd.var_id={} AND r.uid={};
"""

SPATIAL_AGG_BY_DATE_AND_POLYID = """SELECT ST_SummaryStats(ST_Union(ST_Clip(rast, r.geom, TRUE)))
FROM grid_data AS gd, regions AS r
WHERE gd.meta_id={} AND gd.var_id={} AND r.uid={} AND gd.date={};
"""
