create table tmp (
	uid bigserial primary key,
	dataset_id integer,
	var_id integer,
	time_id integer,
	coordX real,
	coordY real,
	rast raster
);

insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 1, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=1;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 2, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=2;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 3, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=3;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 4, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=4;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 5, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=5;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 6, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=6;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 7, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=7;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 8, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=8;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 9, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=9;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 10, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=10;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 11, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=11;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 12, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=12;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 13, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=13;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 14, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=14;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 15, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=15;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 16, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=16;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 17, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=17;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 18, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=18;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 19, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=19;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 20, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=20;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 21, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=21;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 22, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=22;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 23, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=23;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 24, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=24;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 25, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=25;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 26, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=26;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 27, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=27;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 28, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=28;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 29, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=29;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 30, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=30;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 31, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=31;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 32, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=32;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 33, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=33;
insert into tmp (dataset_id, var_id, time_id, rast) select 1, 1, 34, st_tile(rast, 100, 100) from grid_data where dataset_id=1 and var_id=1 and time_id=34;

update tmp set coordX=ST_RasterToWorldCoordX(rast, 0, 0);
update tmp set coordY=ST_RasterToWorldCoordY(rast, 0, 0);

create index on tmp (dataset_id, var_id, time_id);
