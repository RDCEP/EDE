from osgeo import ogr
import sys
import psycopg2
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST
import json
import argparse


def process_shapefile(shapefile):

    ## Connection to the database ##
    conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS,
                            host=DB_HOST, port=DB_PORT)
    cur = conn.cursor()

    reader = ogr.Open(shapefile)
    layer = reader.GetLayer(0)
    layer_name = layer.GetName()

    layer_defn = layer.GetLayerDefn()
    attrs = []
    for i_field in range(layer_defn.GetFieldCount()):
        field_defn = layer_defn.GetFieldDefn(i_field)
        attrs.append(field_defn.GetName())
    attrs = '{\"' + '\",\"'.join(attrs) + '\"}'

    # (1) Insert into regionsets + return uid
    query = "INSERT INTO regionsets (name, attrs) VALUES (\'{}\', \'{}\') returning uid".format(layer_name, attrs)
    cur.execute(query)
    (regionset_id,)= cur.fetchone()

    # (2) Iterate over features
    for i_feature in range(layer.GetFeatureCount()):
        print "ingesting feature no. {}".format(i_feature)
        feature = layer.GetFeature(i_feature).ExportToJson(as_object=True)
        geom = feature['geometry']['coordinates']
        depth_fnc = lambda L: isinstance(L, list) and max(map(depth_fnc, L))+1
        depth = depth_fnc(geom)
        # The case of ordinary, i.e. non-multi polygons
        if depth == 3:
            geom_str = "POLYGON("
            num_rings = len(geom)
            for i_ring in range(num_rings):
                geom_str += '('
                ring = geom[i_ring]
                num_pts = len(ring)
                for i_pt in range(num_pts):
                    geom_str += ' '.join(map(str, ring[i_pt]))
                    if i_pt < num_pts-1:
                        geom_str += ','
                geom_str += ')'
                if i_ring < num_rings-1:
                    geom_str += ','
            geom_str += ')'
        # The case of multi-polygons
        elif depth == 4:
            geom_str = "MULTIPOLYGON("
            num_polys = len(geom)
            for i_poly in range(num_polys):
                geom_str += '('
                poly = geom[i_poly]
                num_rings = len(poly)
                for i_ring in range(num_rings):
                    geom_str += '('
                    ring = poly[i_ring]
                    num_pts = len(ring)
                    for i_pt in range(num_pts):
                        geom_str += ' '.join(map(str, ring[i_pt]))
                        if i_pt < num_pts-1:
                            geom_str += ','
                    geom_str += ')'
                    if i_ring < num_rings-1:
                        geom_str += ','
                geom_str += ')'
                if i_poly < num_polys-1:
                    geom_str += ','
            geom_str += ')'
        else:
            sys.exit("got unexpected nestedness depth of {} in feature".format(depth))

        attrs = json.dumps(feature['properties'])
        attrs = attrs.replace("'", "''")

        # (2) Ingest the feature with its geom + attrs into the regions table
        query = ("INSERT INTO regions (regionset_id, geom, attrs) VALUES ({}, ST_GeomFromText(\'{}\', 4326), \'{}\')".
                 format(regionset_id, geom_str, attrs))
        cur.execute(query)

    conn.commit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Arguments for processing shapefile')
    parser.add_argument('--input', help='Input shapefile', required=True)
    args = parser.parse_args()
    try:
        process_shapefile(args.input)
    except Exception as e:
        print(e)
        print("Could not process shapefile: {}".format(args.input))
        sys.exit()