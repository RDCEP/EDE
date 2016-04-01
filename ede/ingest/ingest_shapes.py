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
        polys = feature['geometry']['coordinates']
        depth_fnc = lambda L: isinstance(L, list) and max(map(depth_fnc, L))+1
        depth = depth_fnc(polys)
        geom_str = "POLYGON(("

        if depth == 3:
            for ring in poly:
                for pt in ring:
                    print pt
        elif depth == 4:
            for poly in polys:
                for ring in poly:
                    for pt in ring:
                        print pt
        else:
            sys.exit("got unexpected nestedness depth of %s in feature" % depth)


        '''
        for p in range(num_pts-1):
            #print "next point: %s" % pts[p]
            geom_str += str(pts[p][0]) # longitude
            geom_str += " "
            geom_str += str(pts[p][1]) # latitude
            geom_str += ", "
        geom_str += str(pts[num_pts-1][0]) # longitude
        geom_str += " "
        geom_str += str(pts[num_pts-1][1]) # latitude
        geom_str = geom_str + "))"

        meta_data = json.dumps(feature['properties'])

        # (2) Ingest the feature with its geom + meta_data into the regions table
        #print geom
        #print geom_str
        query = "insert into regions (meta_id, geom, meta_data) values (%s, ST_GeomFromText(\'%s\'), \'%s\')" % (meta_id, geom_str, meta_data)
        cur.execute(query)
        '''

    #conn.commit()

if __name__ == "__main__":
    shapefile = sys.argv[1]
    main(shapefile)