import sys
from osgeo import ogr


def main(shapefile):
    reader = ogr.Open(shapefile)
    layer = reader.GetLayer(0)
    for i in range(layer.GetFeatureCount()):
        feature = layer.GetFeature(i)
        print feature.ExportToJson()

if __name__ == "__main__":
    shapefile = sys.argv[1]
    main(shapefile)
