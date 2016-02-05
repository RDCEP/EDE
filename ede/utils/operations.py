import op_base
# TODO: PostGIS endpoint

__author__ = "rlourenc@mail.depaul.edu"
# 2016-02-05 - Initial commit


class abstraction:
# Implements the user abstractions of spatial operations. Currently related to use cases.

    def HarvestIndex(self):
        # Implements the Harvest Index for a crop. HI = yield / biomass .
    # Todo: Make query for obtaining a 'yield matrix', and other for a 'biomass' matrix.
        crop_yield = np.matrix()
        crop_biomass = np.matrix()
        hi_matrix = op_base.aggregation.grid_to_grid(crop_yield,crop_biomass)
        return hi_matrix

    def DecadeVariation(simulation, bounding_box, begin_year, end_year):
        # Given a input crop, it returns its variation within decades. Begin and end years are optional (if not
        # supplied return the complete series)
    # Todo: Make query for obtaining the simulation cube

        return
