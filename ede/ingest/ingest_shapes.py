from osgeo import ogr
import sys
import psycopg2
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST


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
    #print query
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        meta_id = int(row[0])

    # (2) Iterate over features
    for i in range(layer.GetFeatureCount()):
        feature = layer.GetFeature(i).ExportToJson(as_object=True)
        geom = feature['geometry']['coordinates']
        print geom
        meta_data = feature['properties']
        print type(meta_data)
        # (2) Ingest the feature with its geom + meta_data into the regions table
        query = "insert into regions (meta_id, geom, meta_data) values (%s, %s, %s)" % (meta_id, geom, meta_data)
        print query
        cur.execute(query)

    conn.commit()

if __name__ == "__main__":
    shapefile = sys.argv[1]
    main(shapefile)
