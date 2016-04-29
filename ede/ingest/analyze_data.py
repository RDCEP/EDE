import os, sys
from netCDF4 import Dataset
from numpy.linalg import norm
import numpy
import numpy.ma as ma

numpy.set_printoptions(threshold='nan')

def has_false(bool_array):
    for b in bool_array:
        if not b:
            return True
    return False


def compute_avg_diff(masked_array):
    data = masked_array.data
    mask = masked_array.mask
    diff_sum = float('nan')
    set = False
    for i in range(data.size):
        if i != data.size-1 and (not mask[i]) and (not mask[i+1]):
            if not set:
                diff_sum = 0
                set = True
            else:
                diff_sum += abs(data[i+1]-data[i])
    if not set:
        return float('nan')
    else:
        return diff_sum / data.size

def main(netcdf_filename):

    rootgrp = Dataset(netcdf_filename, "r", format="NETCDF4")

    lons = rootgrp.variables['lon']
    lats = rootgrp.variables['lat']
    times = rootgrp.variables['time']
    yield_whe = rootgrp.variables['yield_whe'][:]
    num_pixels = lons.size * lats.size

    num_null = 0
    count_valid = 0
    count_0 = 0
    accuracy_level_1 = 0.0001
    count_1 = 0
    accuracy_level_2 = 0.001
    count_2 = 0
    accuracy_level_3 = 0.01
    count_3 = 0
    accuracy_level_4 = 0.1
    count_4 = 0
    for lat in range(lats.size):
        for lon in range(lons.size):
            vals = yield_whe[:, lat, lon]
            if has_false(vals.mask):
                # has at least one valid value
                num_null += 1
                #print vals
                avg_diff = compute_avg_diff(vals)
                if avg_diff:
                    count_valid += 1
                    if avg_diff < accuracy_level_1:
                        count_0 += 1
                    elif accuracy_level_1 <= avg_diff < accuracy_level_2:
                        count_1 += 1
                    elif accuracy_level_2 <= avg_diff < accuracy_level_3:
                        count_2 += 1
                    elif accuracy_level_3 <= avg_diff < accuracy_level_4:
                        count_3 += 1
                    else:
                        count_4 += 1

    print "Ratio for accuracy in [0, %f]: %f" % (accuracy_level_1, count_0)
    print "Ratio for accuracy in [%f, %f]: %f" % (accuracy_level_1, accuracy_level_2, count_1)
    print "Ratio for accuracy in [%f, %f]: %f" % (accuracy_level_2, accuracy_level_3, count_2)
    print "Ratio for accuracy in [%f, %f]: %f" % (accuracy_level_3, accuracy_level_4, count_3)
    print "Ratio for accuracy in [%f, ...]: %f" % (accuracy_level_4, count_4)
    print "Total number of valid average differences: %f" % count_valid
    print "Out of %d point slices %d were completely null" % (num_pixels, num_null)


    for t in range(times.size):
        vals = yield_whe[t, :, :]
        print "Frame %d has %d vals out of %d that are null" % (t, ma.count_masked(vals), num_pixels)

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
