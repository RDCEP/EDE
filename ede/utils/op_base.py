import numpy as np
from scipy import stats
#import sklearn as sk

__author__ = "rlourenc@mail.depaul.edu"
# 2016-02-05 - Initial commit
# Implements base spatial operations for abstraction consumption.


class aggregation:
    def grid_to_grid(self, grid1, grid2):
        agg_grid = np.divide((np.matrix(grid1)+np.matrix(grid2)),2.)
        return agg_grid

    def grid_to_poly():
        return

    def poly_to_poly():
        return
    return

class binning:
    def grid_to_grid(self, grid1, grid2):
        binned_grid = stats.
        return binned_grid

    def grid_to_poly():
        return

    def poly_to_poly():
        return
    return

class normalization:
    def grid_to_grid(self, grid1, grid2):
        # Note: Matrices must be on the same dimensions. If grid2 doesn't contains floating ponit numbers,
        # the division will be rounded to the closest integer. Zero values on grid2 will generate NaN as output.
        norm_grid = np.divide(np.matrix(grid1),np.matrix(grid2))
        return norm_grid

    def grid_to_poly():
        return

    def poly_to_poly():
        return
    return

