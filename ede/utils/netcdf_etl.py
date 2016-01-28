# -*- coding: utf-8 -*-
from datetime import datetime
from boto.s3.connection import S3ResponseError
from ede.database import session
from ede.utils.netcdffile import import_netcdffile, NetcdffileError
# TODO: abstract this ETLFile class into a separate file, e.g. put it into etl.py
from ede.utils.shape_etl import ETLFile
from ede.models import NetcdfMetadata
from ede.utils.etl import EDE_ETLError

class NetcdfETL:

    def __init__(self, meta, source_path=None, save_to_s3=False):
        self.save_to_s3 = save_to_s3
        self.source_path = source_path
        self.table_name = meta.dataset_name
        self.source_url = meta.source_url
        self.meta = meta

    # Gets the metadata of the NetCDF out of the database
    def _get_metadata(self):
        netcdf_meta = session.query(NetcdfMetadata).get(self.table_name)
        if not netcdf_meta:
            raise EDE_ETLError("Table {} is not registered in the metadata.".format(self.table_name))
        return netcdf_meta

    def _refresh_metadata(self):
        pass

    # Ingests the NetCDF into the database and updates the metadata
    # (e.g. the updated bounding box)
    def import_netcdffile(self):
        if self.meta.is_ingested:
            raise EDE_ETLError("Table {} has already been ingested.".format(self.table_name))

        # NB: this function is not atomic.
        # update_after_ingest could fail after _ingest_metadata succeeds, leaving us with inaccurate metadata.
        # If this becomes a problem, we can tweak the ogr2ogr import to return a big SQL string
        # rather than just going ahead and importing the netcdffile.
        # Then we could put both operations in the same transaction.
        self._ingest_netcdffile()
        self.meta.update_after_ingest(session)
        session.commit()

    # Ingests the NetCDF into the database
    def _ingest_netcdffile(self):

        def attempt_save_to_s3(file_helper):
            try:
                # Use current time to create uniquely named file in S3 bucket
                now_timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                s3_path = '{}/{}.zip'.format(self.table_name, now_timestamp)
                file_helper.upload_to_s3(s3_path)
            except S3ResponseError as e:
                # If AWS storage fails, soldier on.
                print "Failed to upload file to S3.\n" + e.message

        # Get a handle to the netcdf.
        with ETLFile(source_url=self.source_url, source_path=self.source_path) as netcdffile:

            # Try to save to S3 first so that we have a record of what the dataset looked like
            # even if insertion fails.
            if self.save_to_s3:
                attempt_save_to_s3(netcdffile)

            # Attempt insertion
            try:
                    # TODO: Make sure the filename : netcdffile.handle.name of the downloaded NetCDF
                    # works with ogr2ogr later. The reason for using the filename instead of the file handle
                    # is that ogr2ogr ultimately needs a filename
                    import_netcdffile(netcdffile.handle.name, self.table_name)
            except NetcdffileError as e:
                raise EDE_ETLError("Failed to import NetCDF file.\n{}".format(repr(e)))
