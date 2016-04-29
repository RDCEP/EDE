import os, sys
from netCDF4 import Dataset
from numpy.linalg import norm
import numpy

numpy.set_printoptions(threshold='nan')

def has_false(bool_array):
    for b in bool_array:
        if not b:
            return True
    return False


def count_false(bool_array):
    res = 0
    for b in bool_array:
        if not b:
            res += 1
    return res

def main(netcdf_filename):

    rootgrp = Dataset(netcdf_filename, "r", format="NETCDF4")

    lons = rootgrp.variables['lon']
    lats = rootgrp.variables['lat']
    times = rootgrp.variables['time']
    yield_whe = rootgrp.variables['yield_whe'][:]
    num_pixels = lons.size * lats.size

    num_not_null = 0
    for lat in range(lats.size):
        for lon in range(lons.size):
            vals = yield_whe[:, lat, lon]
            if has_false(vals.mask):
                # has at least one valid value
                num_not_null += 1
                print vals

    print "Out of %d point slices %d were not completely null" % (num_pixels, num_not_null)


    for t in range(times.size):
        vals = yield_whe[t]
        print "Frame %d has %d vals out of %d that were not completely null" % (t, count_false(vals), num_pixels)


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
