import numpy as np
#import sklearn as sk
from scipy import stats
#from shapely import ops, geometry


__author__ = "rlourenc@mail.depaul.edu"
# 2016-02-05 - Initial commit
# Implements base spatial operations for abstraction consumption.
#Todo: Grid to poly operations must be specified on usecases prior to implementation

class aggregation:
    def grid_to_grid(self, cube):
        # Operation applied for a 3D cube. Cube is on EPSG order
        # (0, 1, 2, being latitude, longitude, time), being collapsed on time.
        # Null values will be discarded.
        agg_grid = np.apply_over_axes(np.nanmean,cube,2)
        return agg_grid

    def grid_to_poly(self):
        return

class binning:
    def grid_to_grid(self, cube, bin_range):
        binned_cube = np.apply_over_axes(np.histogram(bins=bin_range), cube, 2)
        return binned_cube

    def grid_to_poly(self):

        return

class normalization:
    def grid_to_grid(self, grid1, grid2):
        # Note: Matrices must be on the same dimensions. If grid2 doesn't contains floating ponit numbers,
        # the division will be rounded to the closest integer. Zero values on grid2 will generate NaN as output.
        norm_grid = np.divide(np.matrix(grid1),np.matrix(grid2))
        return norm_grid

    def grid_to_poly(self):
        return