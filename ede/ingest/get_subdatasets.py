import sys, time
from osgeo import gdal

def main(in_filename, out_filename):

    gdal_dataset = gdal.Open(in_filename)
    out_file = open(out_filename, 'w')

    for sd in gdal_dataset.GetSubDatasets():
        out_file.write(sd[0] + '\n')

if __name__ == "__main__":
    in_filename = sys.argv[1]
    out_filename = in_filename + '_subdatasets'
    start = time.time()
    main(in_filename, out_filename)
    end = time.time()
    print "Elapsed time: %.2f" % (end-start)