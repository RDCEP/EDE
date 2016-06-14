CREATE TABLE grid_data_psims_time_lat_lon (
	uid bigserial primary key,
	var_id integer,
	time_id integer,
	rast raster
);
