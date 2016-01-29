from ede.utils.ogr2ogr import import_netcdffile_to_table, OgrError

class NetcdffileError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message

def import_netcdffile(netcdf_filename, table_name):
    """
    :param netcdf_filename: The netcdf filename.
    """
    try:
        import_netcdffile_to_table(netcdf_filename, table_name)
    except OgrError as e:
        raise NetcdffileError('Failed to insert netcdffile into database.\n{}'.format(repr(e)))
