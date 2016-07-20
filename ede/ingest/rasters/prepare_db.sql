-- Fill raster_data_series from raster_data_single
INSERT INTO raster_data_series(dataset_id, var_id, geom, values)
SELECT dataset_id, var_id, geom, array_agg(value) from raster_data_single GROUP BY dataset_id, var_id, geom;

-- Create indexes on raster_data_single
CREATE INDEX raster_data_single_dataset_id_var_id_idx ON raster_data_single(dataset_id, var_id);
CREATE INDEX raster_data_single_geom_idx ON raster_data_single USING GIST (geom);
CREATE INDEX raster_data_single_time_id_idx ON raster_data_single(time_id);

-- Create indexes on raster_data_series
CREATE INDEX raster_data_series_dataset_id_var_id_idx ON raster_data_series(dataset_id, var_id);
CREATE INDEX raster_data_series_geom_idx ON raster_data_series USING GIST (geom);