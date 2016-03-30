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

'''
## Connection to the database ##
conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS,
                            host=DB_HOST, port=DB_PORT)
cur = conn.cursor()
'''

def main(shapefile):
    reader = ogr.Open(shapefile)

    print "type of reader..."
    print type(reader)


    layer = reader.GetLayer(0)
    for i in range(layer.GetFeatureCount()):
        feature = layer.GetFeature(i).ExportToJson(as_object=True)
        print "geometry..."
        print feature['geometry']['coordinates'] # the coordinates, POLYGON of that + GeomFromText
        print "properties..."
        print feature['properties'] # should be ingestable as the meta_data JSON field

    '''
    cur.execute("insert into regions_meta (name, attributes) values (%s, %s) returning uid" % ())
    rows = cur.fetchall()
    for row in rows:
        meta_id = int(row[0])
    '''


if __name__ == "__main__":
    shapefile = sys.argv[1]
    main(shapefile)
