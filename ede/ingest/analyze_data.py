import os, sys
from netCDF4 import Dataset
from numpy.linalg import norm
import numpy

numpy.set_printoptions(threshold='nan')

def main(netcdf_filename):

    rootgrp = Dataset(netcdf_filename, "r", format="NETCDF4")

    lons = rootgrp.variables['lon']
    lats = rootgrp.variables['lat']
    yield_whe = rootgrp.variables['yield_whe']
    for lat in range(lats.size):
        for lon in range(lons.size):
            print yield_whe[:, lat, lon]

    # time = rootgrp.variables['time']
    # yield_whe_frame_prev = None
    # yield_whe_frame_next = None
    # for t in range(time.size):
    #     yield_whe_frame_prev = yield_whe_frame_next
    #     yield_whe_frame_next = rootgrp.variables['yield_whe'][t]
    #     if yield_whe_frame_prev is not None and yield_whe_frame_next is not None:
    #         print yield_whe_frame_next
    #         print "Euclidean difference: %f" % norm(yield_whe_frame_next-yield_whe_frame_next)

    rootgrp.close()

if __name__ == "__main__":
    netcdf_filename = "/var/www/ede_unstacked/ede/data/atlas/papsim_wfdei.cru_hist_harmnon_noirr_yield_whe_annual_1979_2012.nc4"
    main(netcdf_filename)
