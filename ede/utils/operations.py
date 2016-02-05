from datetime import datetime
from boto.s3.connection import S3ResponseError
from ede.database import session
import numpy as np
# TODO: PostGIS endpoint
#import sklearn as sk

class EdeOp:
    # TODO: Implement init (use cases abstraction)
    def __init__(self):
        self.data = []
        return

    def abstraction(self):
        # Implements the user abstractions of spatial operations. Currently related to use cases.

        def HarvestIndex(simulation, bouding_box, time):
        # Todo: Make query for obtaining a 'yield matrix', and other for a 'biomass' matrix.
            crop_yield = np.matrix()
            crop_biomass = np.matrix()
            return

        def DecadeVariation(grid_variable):
            return

        return

    #TODO: Implement spatial operations for abstraction consumption

    def base_op(self):
        # Implements base spatial operations for abstraction consumption.


        def aggregation(self):
            def grid_to_grid(grid1, grid2):
                agg_grid = np.divide((np.matrix(grid1)+np.matrix(grid2)),2.)
                return agg_grid

            def grid_to_poly():
                return

            def poly_to_poly():
                return
            return

        def normalization():
            def grid_to_grid(grid1, grid2):
                # Note: Matrices must be on the same dimensions. If grid2 doesn't contains floating ponit numbers,
                # the division will be rounded to the closest integer. Zero values on grid2 will generate NaN as output.
                agg_grid = np.divide(np.matrix(grid1),np.matrix(grid2))
                return agg_grid

            def grid_to_poly():
                return

            def poly_to_poly():
                return
            return

        return
