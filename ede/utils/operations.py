import op_base
import datetime
# TODO: PostGIS endpoint
# Todo: Prior to sending GeoJSON messages, treat all NaN for Null (in adhrence to JSON standard)

__author__ = "rlourenc@mail.depaul.edu"
# 2016-02-05 - Initial commit


class abstraction:
# Implements the user abstractions of spatial operations. Currently related to use cases.

    def HarvestIndex(self, simulation, bounding_box, time_slice):
        # Implements the Harvest Index for a crop. HI = yield / biomass .
    # Todo: Make query for obtaining a 'yield matrix', and other for a 'biomass' matrix.
        crop_yield = np.matrix() # use on this call, simulation and bounding box
        crop_biomass = np.matrix()
        hi_matrix = op_base.normalization.grid_to_grid(crop_yield,crop_biomass)
        return hi_matrix

    def DecadeVariation(self, simulation, bounding_box, begin_year, end_year):
        # Given a input crop, it returns its variation within decades. Begin and end years are optional (if not
        # supplied return the complete series)
    # Todo: Make query for obtaining the simulation cube
        ts_cube = np.matrix() #using bounding box and simulation to call
        begin = datetime.timedelta(begin_year, 1, 1)
        end = datetime.timedelta(end_year, 1, 1)
        bin_range = begin - end
        bin_cube = op_base.binning.grid_to_grid(ts_cube, bin_range)
        return bin_cube
