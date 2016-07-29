-- Create indexes on raster_data
CREATE INDEX raster_data_dataset_id_var_id_idx ON raster_data(dataset_id, var_id);
CREATE INDEX raster_data_time_id_idx ON raster_data(time_id);
CREATE INDEX raster_data_rast_idx ON raster_data USING GIST (ST_ConvexHull(rast));