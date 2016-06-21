CREATE TABLE grid_data (
	uid bigserial primary key,
	dataset_id integer,
	var_id integer,
	time_id integer,
	rast raster
);
