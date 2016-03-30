import sys
from osgeo import ogr


def main(shapefile):
    reader = ogr.Open(shapefile)
    layer = reader.GetLayer(0)
    for i in range(layer.GetFeatureCount()):
        feature = layer.GetFeature(i).ExportToJson()
        print "geometry..."
        print feature['geometry']['coordinates']
        print "properties..."
        print feature['properties']

if __name__ == "__main__":
    shapefile = sys.argv[1]
    main(shapefile)
