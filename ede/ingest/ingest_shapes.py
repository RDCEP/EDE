from osgeo import ogr
import sys
import psycopg2
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST
import json


def main(shapefile):

    ## Connection to the database ##
    conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS,
                            host=DB_HOST, port=DB_PORT)
    cur = conn.cursor()

    reader = ogr.Open(shapefile)
    layer = reader.GetLayer(0)
    layer_name = layer.GetName()

    layer_defn = layer.GetLayerDefn()
    attrs = []
    for i in range(layer_defn.GetFieldCount()):
        field_defn = layer_defn.GetFieldDefn(i)
        attrs.append(field_defn.GetName())
    attrs = '{\"' + '\",\"'.join(attrs) + '\"}'

    # (1) Insert into regions_meta + return uid as meta_id
    query = "insert into regions_meta (name, attributes) values (\'%s\', \'%s\') returning uid" % (layer_name, attrs)
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        meta_id = int(row[0])

    # (2) Iterate over features
    for i in range(layer.GetFeatureCount()):
        feature = layer.GetFeature(i).ExportToJson(as_object=True)
        geom = feature['geometry']['coordinates']
        depth_fnc = lambda L: isinstance(L, list) and max(map(depth_fnc, L))+1
        depth = depth_fnc(geom)
        # The case of non-multi-polygons
        if depth == 3:
            geom_str = "POLYGON("
            num_rings = len(geom)
            for i_ring in range(num_rings):
                geom_str += '('
                ring = geom(i_ring)
                num_pts = len(ring)
                for i_pt in range(num_pts):
                    geom_str += ' '.join(map(str, ring[i_pt]))
                    if i_pt < num_pts-1:
                        geom_str += ','
                geom_str += ')'
                if i_ring < num_rings-1:
                    geom_str += ','
            geom_str += ')'

            print geom_str

        # The case of multi-polygons
        elif depth == 4:
            geom_str = "MULTIPOLYGON("
            num_polys = len(geom)
            for i_poly in range(num_polys):
                geom_str += '('
                poly = geom(i_poly)
                num_rings = len(poly)
                for i_ring in range(num_rings):
                    geom_str += '('
                    ring = poly(i_ring)
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

            print geom_str

        else:
            sys.exit("got unexpected nestedness depth of %s in feature" % depth)

        meta_data = json.dumps(feature['properties'])
        # (2) Ingest the feature with its geom + meta_data into the regions table
        query = "insert into regions (meta_id, geom, meta_data) values (%s, ST_GeomFromText(\'%s\'), \'%s\')" % (meta_id, geom_str, meta_data)
        cur.execute(query)

    #conn.commit()

if __name__ == "__main__":
    shapefile = sys.argv[1]
    main(shapefile)