from datetime import datetime
from boto.s3.connection import S3ResponseError
from ede.database import session
import numpy as np
# TODO: PostGIS endpoint
#import sklearn as sk

class EdeOp:
    # TODO: Implement init (use cases abstraction)
    def __init__(self):
        #self.
        return

    def abstraction(self):
        # Implements the user abstractions of spatial operations. Currently related to use cases.

        def HarvestIndex(self):
            #    Implement a harvest index operation. Pointwise calculation between yield and biomass.
            # Could be for an specific time slice or give the full result.
            return



        return

    #TODO: Implement spatial operations for abstraction consumption

    def base_op(self):
        # Implements base spatial operations for abstraction consumption

        def aggregation():
            def grid_to_grid():
                return

            def grid_to_poly():
                return

            def poly_to_poly():
                return
            return

        return
