-- Create indexes on raster_data
CREATE INDEX raster_data_dataset_id_var_id_idx ON raster_data(dataset_id, var_id);
CREATE INDEX raster_data_rast_idx ON raster_data USING GIST (ST_ConvexHull(rast));

UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 1) where ST_BandIsNoData(rast, 1, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 2) where ST_BandIsNoData(rast, 2, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 3) where ST_BandIsNoData(rast, 3, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 4) where ST_BandIsNoData(rast, 4, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 5) where ST_BandIsNoData(rast, 5, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 6) where ST_BandIsNoData(rast, 6, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 7) where ST_BandIsNoData(rast, 7, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 8) where ST_BandIsNoData(rast, 8, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 9) where ST_BandIsNoData(rast, 9, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 10) where ST_BandIsNoData(rast, 10, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 11) where ST_BandIsNoData(rast, 11, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 12) where ST_BandIsNoData(rast, 12, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 13) where ST_BandIsNoData(rast, 13, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 14) where ST_BandIsNoData(rast, 14, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 15) where ST_BandIsNoData(rast, 15, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 16) where ST_BandIsNoData(rast, 16, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 17) where ST_BandIsNoData(rast, 17, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 18) where ST_BandIsNoData(rast, 18, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 19) where ST_BandIsNoData(rast, 19, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 20) where ST_BandIsNoData(rast, 20, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 21) where ST_BandIsNoData(rast, 21, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 22) where ST_BandIsNoData(rast, 22, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 23) where ST_BandIsNoData(rast, 23, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 24) where ST_BandIsNoData(rast, 24, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 25) where ST_BandIsNoData(rast, 25, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 26) where ST_BandIsNoData(rast, 26, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 27) where ST_BandIsNoData(rast, 27, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 28) where ST_BandIsNoData(rast, 28, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 29) where ST_BandIsNoData(rast, 29, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 30) where ST_BandIsNoData(rast, 30, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 31) where ST_BandIsNoData(rast, 31, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 32) where ST_BandIsNoData(rast, 32, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 33) where ST_BandIsNoData(rast, 33, TRUE);
UPDATE raster_data SET rast = ST_SetBandIsNoData(rast, 34) where ST_BandIsNoData(rast, 34, TRUE);

CREATE TYPE int_double_tuple AS(
	id integer,
	val double precision
);

CREATE FUNCTION fill_up_with_nulls(
array_in int_double_tuple[],time_id_start int,
time_id_step int,num_times int)
RETURNS double precision[] AS $$
DECLARE
	array_out double precision[];
	x int_double_tuple;
	i int;
	j int;
BEGIN
	FOR i IN 1..num_times
	LOOP
		array_out[i]:=NULL;
	END LOOP;
	FOREACH x IN ARRAY array_in
	LOOP
		j:=(x.id-time_id_start)/time_id_step+1;
		array_out[j]:=x.val;
	END LOOP;
	RETURN array_out;
END;
$$ LANGUAGE plpgsql;