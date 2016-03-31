import sys
from osgeo import ogr

import os, sys, subprocess, time
from netCDF4 import Dataset
from osgeo import gdal
import psycopg2
from psycopg2.extras import Json
import re
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST
from datetime import datetime, timedelta


def main(shapefile):

    ## Connection to the database ##
    conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS,
                            host=DB_HOST, port=DB_PORT)
    cur = conn.cursor()

    reader = ogr.Open(shapefile)
    layer = reader.GetLayer(0)
    layer_name = layer.GetName()

    layer_defn = layer.GetLayerDefn()
    for i in range(layer_defn.GetFieldCount()):
        field_defn = layer_defn.GetFieldDefn(i)
        print field_defn.GetName()

    # (1) Insert into regions_meta + return uid as meta_id
    cur.execute("insert into regions_meta (name, attributes) values (%s, %s) returning uid" % (layer_name, attrs))
    rows = cur.fetchall()
    for row in rows:
        meta_id = int(row[0])

    # (2) Iterate over features
    for i in range(layer.GetFeatureCount()):
        feature = layer.GetFeature(i).ExportToJson(as_object=True)
        geom = feature['geometry']['coordinates']
        meta_data = feature['properties']
        # (2) Ingest the feature with its geom + meta_data into the regions table
        cur.execute("insert into regions (meta_id, geom, meta_data) values (%s, %s, %s)" % (meta_id, geom, meta_data))

if __name__ == "__main__":
    shapefile = sys.argv[1]
    main(shapefile)
