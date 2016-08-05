-- Create indexes on raster_data
CREATE INDEX raster_data_dataset_id_var_id_idx ON raster_data(dataset_id, var_id);
CREATE INDEX raster_data_time_id_idx ON raster_data(time_id);
CREATE INDEX raster_data_rast_idx ON raster_data USING GIST (ST_ConvexHull(rast));

-- Helper type + function, needed by the selection time range query
CREATE TYPE int_double_tuple AS (
    id integer,
    val double precision
);

CREATE FUNCTION fill_up_with_nulls(array_in int_double_tuple[], time_id_start int, time_id_step int, num_times int)
RETURNS double precision[] AS $$
DECLARE
	array_out double precision[];
	x int_double_tuple;
	i int;
	j int;
BEGIN
	FOR i IN 1 .. num_times
	LOOP
		array_out[i] := NULL;
	END LOOP;
	FOREACH x IN ARRAY array_in
	LOOP
		j := ( x.id - time_id_start ) / time_id_step + 1;
		array_out[j] := x.val;
	END LOOP;
	RETURN array_out;
END;
$$ LANGUAGE plpgsql;