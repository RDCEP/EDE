-- Helper types + functions for getting ingestion into raster_data_series right
CREATE TYPE int_double_tuple AS (
    id integer,
    val double precision
);

CREATE OR REPLACE FUNCTION array_int_double_sort(int_double_tuple[])
RETURNS double precision[] AS $$
 SELECT ARRAY(SELECT (id_val).val FROM (SELECT unnest($1) AS id_val) AS foo ORDER BY (id_val).id)
$$ LANGUAGE sql;

-- Fill raster_data_series from raster_data_single
-- no longer needed since we're doing it (hardcodedly) in the ingestion script!!
-- INSERT INTO raster_data_series(dataset_id, var_id, geom, values)
-- SELECT dataset_id, var_id, geom, array_int_double_sort(array_agg((time_id, value)::int_double_tuple))
-- FROM raster_data_single GROUP BY dataset_id, var_id, geom;

-- Create indexes on raster_data_single
CREATE INDEX raster_data_single_dataset_id_var_id_idx ON raster_data_single(dataset_id, var_id);
CREATE INDEX raster_data_single_geom_idx ON raster_data_single USING GIST (geom);
CREATE INDEX raster_data_single_time_id_idx ON raster_data_single(time_id);

-- Create indexes on raster_data_series
CREATE INDEX raster_data_series_dataset_id_var_id_idx ON raster_data_series(dataset_id, var_id);
CREATE INDEX raster_data_series_geom_idx ON raster_data_series USING GIST (geom);

-- Create user-defined functions needed for queries
CREATE OR REPLACE FUNCTION array_avg(double precision[])
RETURNS double precision AS $$
SELECT avg(v) FROM unnest($1) g(v)
$$ LANGUAGE sql;