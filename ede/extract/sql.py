#!/usr/bin/env python
# -*- coding: utf-8 -*-


GRID_META = """ SELECT uid, filename, filesize, filetype, meta_data,
date_created, date_inserted FROM grid_meta
"""

GRID_META_BY_UID = GRID_META + " WHERE uid in {} "

MAKE_POLY = """ ST_Polygon(ST_GeomFromText(
'LINESTRING({} {}, {} {}, {} {}, {} {}, {} {})'), 4326)
"""

GRID_DATA = " SELECT ST_X(g.geom), ST_Y(g.geom), g.val "
GRID_DATA += """ FROM (SELECT(ST_PixelAsCentroids(ST_Clip(rast, {}, TRUE))).*
FROM grid_data
"""
GRID_DATA_BY_DATE = GRID_DATA + """ WHERE meta_id={} AND var_id={} AND date={})
AS g foo
"""
GRID_DATA_BY_DATE_AND_POLYID = GRID_DATA + """FROM grid_data AS gd,
regions AS r
WHERE gd.meta_id={} AND gd.var_id={} AND r.uid={} AND gd.date={})
AS g foo;
"""
GRID_DATA = GRID_DATA + " WHERE meta_id={} AND var_id={}) AS g foo "

G = """SELECT ST_X(geom), ST_Y(geom), val
FROM (SELECT(ST_PixelAsCentroids(ST_Clip(rast, r.geom, TRUE))).*
FROM grid_data AS gd, regions AS r
WHERE gd.meta_id={} AND gd.var_id={} AND r.uid={} AND gd.date={}) foo;"""


DATE = "SELECT to_char(date, \'YYYY-MM-DD HH24:MI:SS\') from grid_dates"
DATE_BY_ID = DATE + "WHERE uid={}"