-------------------------
-- General Assumptions --
-------------------------
-- The below commands were written under the assumption that the ingestion procedure is done in the following steps:
-- (1) pSIMS
-- (1.1) Ingest the listed files
-- (1.2) Execute the correction commands
-- (2) AgMERRA
-- (2.1) Ingest the listed files
-- (2.2) Execute the correction commands
-- (3) GSDE
-- (3.1) Ingest the listed files
-- (3.2) Execute the correction commands

-----------
-- pSIMS --
-----------

-- files

-- Ingest the following pSIMS files in this order (Note that the ingestion code must detect "growing seasons"):
-- papsim_wfdei.cru_hist_harmnon_noirr_yield_whe_annual_1979_2012.nc4
-- papsim_wfdei.cru_hist_harmnon_firr_biom_whe_annual_1979_2012.nc4
-- papsim_wfdei.cru_hist_fullharm_firr_pm_maty-day_whe_annual_1979_2012.nc4
-- papsim_wfdei.cru_hist_default_noirr_maty-day_whe_annual_1979_2012.nc4
-- papsim_wfdei.cru_hist_fullharm_noirr_aet_whe_annual_1979_2012.nc4
-- papsim_wfdei.cru_hist_default_firr_pirrww_whe_annual_1979_2012.nc4

-- correction commands, wrapped in a transaction

BEGIN;

-- delete from dataset
DELETE FROM dataset WHERE 2<=uid AND uid<=6;

-- delete from value_1d
DELETE FROM value_1d WHERE var_id IN (SELECT uid FROM variable WHERE 2<=dataset_id AND dataset_id<=6
AND name=ANY('{"time","lat","lon"}'));

-- delete from variable
DELETE FROM variable WHERE 2<=dataset_id AND dataset_id<=6 AND name=ANY('{"time","lat","lon"}');

-- update dataset_id for variables we keep
UPDATE variable SET dataset_id=1 WHERE 2<=dataset_id AND dataset_id<=6 AND name!=ANY('{"time","lat","lon"}');

COMMIT;


-------------
-- AgMERRA --
-------------

-- files

-- Ingest the following AgMERRA files in this order:
-- clim_0004_0047.tile.nc4
-- clim_0004_0048.tile.nc4
-- clim_0005_0047.tile.nc4
-- clim_0005_0048.tile.nc4

-- correction commands, wrapped in a transaction

BEGIN;

-- delete from dataset
DELETE FROM dataset WHERE 8<=uid AND uid<=10;

-- relink data in value_lat_lon (cropland variable)
-- SELECT uid FROM variable WHERE 7<= dataset_id AND dataset_id<=10 AND name='cropland'; // e.g.: 35,46,57,68
UPDATE value_lat_lon SET var_id=35 WHERE var_id=46;
UPDATE value_lat_lon SET var_id=35 WHERE var_id=57;
UPDATE value_lat_lon SET var_id=35 WHERE var_id=68;

-- relink data in value_time_lat_lon (all other variables)
-- use temporary index to be faster
-- SELECT uid,name FROM variable WHERE dataset_id=7 AND axes='{"T","Y","X"}');
-- SELECT uid,name FROM variable WHERE name='pr'; // 27,38,49,60
-- SELECT uid,name FROM variable WHERE name='tasmax'; // 29,40,51,62
-- SELECT uid,name FROM variable WHERE name='tasmin'; // 30,41,52,63
-- SELECT uid,name FROM variable WHERE name='rsds'; // 31,42,53,64
-- SELECT uid,name FROM variable WHERE name='wind'; // 32,43,54,65
-- SELECT uid,name FROM variable WHERE name='hur'; // 33,44,55,66
-- SELECT uid,name FROM variable WHERE name='hurtmax'; // 34,45,56,67
CREATE index value_time_lat_lon_var_id_idx ON value_time_lat_lon(var_id);
UPDATE value_time_lat_lon SET var_id=27 WHERE var_id=ANY('{38,49,60}');
UPDATE value_time_lat_lon SET var_id=29 WHERE var_id=ANY('{40,51,62}');
UPDATE value_time_lat_lon SET var_id=30 WHERE var_id=ANY('{41,52,63}');
UPDATE value_time_lat_lon SET var_id=31 WHERE var_id=ANY('{42,53,64}');
UPDATE value_time_lat_lon SET var_id=32 WHERE var_id=ANY('{43,54,65}');
UPDATE value_time_lat_lon SET var_id=33 WHERE var_id=ANY('{44,55,66}');
UPDATE value_time_lat_lon SET var_id=34 WHERE var_id=ANY('{45,56,67}');
DROP index value_time_lat_lon_var_id_idx;

-- relink data in value_1d: time(time),lat(lat),lon(lon)

-- time(time)
-- SELECT uid,name FROM variable WHERE 7<=dataset_id AND dataset_id<=10 AND name='time';
DELETE FROM value_1d WHERE var_id=ANY('{39,50,61}');

-- lat(lat)
-- SELECT uid,name FROM variable WHERE dataset_id=ANY('{8,10}') AND name='lat'; // 36,58
DELETE FROM value_1d WHERE var_id=ANY('{36,58}');
-- SELECT uid,name FROM variable WHERE dataset_id=9 AND name='lat'; // 47
UPDATE value_1d SET index_0=index_0+8 WHERE var_id=47;
-- SELECT uid,name FROM variable WHERE dataset_id=7 AND name='lat'; // 25
UPDATE value_1d SET var_id=25 WHERE var_id=47;

-- lon(lon)
-- SELECT uid,name FROM variable WHERE dataset_id=ANY('{9,10}') AND name='lon'; // 48,59
DELETE FROM value_1d WHERE var_id=ANY('{48,59}');
-- SELECT uid,name FROM variable WHERE dataset_id=8 AND name='lon'; // 37
UPDATE value_1d SET index_0=index_0+8 WHERE var_id=37;
-- SELECT uid,name FROM variable WHERE dataset_id=7 AND name='lon'; // 26
UPDATE value_1d SET var_id=26 WHERE var_id=37;

-- relink data in value_2d: cropland(lat,lon)
-- SELECT uid,name FROM variable WHERE 7<=dataset_id AND dataset_id<=10 AND name='cropland'; // 35,46,57,68
UPDATE value_2d SET index_1=index_1+8 WHERE var_id=46;
UPDATE value_2d SET index_0=index_0+8 WHERE var_id=57;
UPDATE value_2d SET index_0=index_0+8, index_1=index_1+8 WHERE var_id=68;
UPDATE value_2d SET var_id=35 WHERE var_id=ANY('{46,57,68}');

-- relink data in value_3d: all other var(time,lat,lon)
-- use temporary index to be faster
-- update lat,lon indexes
CREATE index value_3d_var_id_idx ON value_3d(var_id);
UPDATE value_3d SET index_2=index_2+8 WHERE var_id=ANY('{38,40,41,42,43,44,45}');
UPDATE value_3d SET index_1=index_1+8 WHERE var_id=ANY('{49,51,52,53,54,55,56}');
UPDATE value_3d SET index_1=index_1+8, index_2=index_2+8 WHERE var_id=ANY('{60,62,63,64,65,66,67}');
-- relink var_id to point to representative
UPDATE value_3d SET var_id=27 WHERE var_id=ANY('{38,49,60}');
UPDATE value_3d SET var_id=29 WHERE var_id=ANY('{40,51,62}');
UPDATE value_3d SET var_id=30 WHERE var_id=ANY('{41,52,63}');
UPDATE value_3d SET var_id=31 WHERE var_id=ANY('{42,53,64}');
UPDATE value_3d SET var_id=32 WHERE var_id=ANY('{43,54,65}');
UPDATE value_3d SET var_id=33 WHERE var_id=ANY('{44,55,66}');
UPDATE value_3d SET var_id=34 WHERE var_id=ANY('{45,56,67}');
DROP index value_3d_var_id_idx;

-- recompute min,max info + some other metadata for vars

-- update dims_sizes
-- SELECT uid,name FROM variable WHERE dataset_id=7;
UPDATE variable SET dims_sizes='{16}' WHERE dataset_id=7 AND name='lat';
UPDATE variable SET dims_sizes='{16}' WHERE dataset_id=7 AND name='lon';
UPDATE variable SET dims_sizes='{16,16}' WHERE dataset_id=7 AND name='cropland';
-- SELECT uid,name,dims_sizes FROM variable WHERE dataset_id=7 AND NOT (name=ANY('{"time","lat","lon","cropland"}'));
UPDATE variable SET dims_sizes='{11323,16,16}' WHERE dataset_id=7 AND NOT (name=ANY('{"time","lat","lon","cropland"}'));

-- update min,max
-- for lat
UPDATE variable SET min=(SELECT min(value) FROM value_1d WHERE var_id=25) WHERE uid=25;
UPDATE variable SET max=(SELECT max(value) FROM value_1d WHERE var_id=25) WHERE uid=25;
-- for lon
UPDATE variable SET min=(SELECT min(value) FROM value_1d WHERE var_id=26) WHERE uid=26;
UPDATE variable SET max=(SELECT max(value) FROM value_1d WHERE var_id=26) WHERE uid=26;
-- for cropland
UPDATE variable SET min=(SELECT min(value) FROM value_2d WHERE var_id=35) WHERE uid=35;
UPDATE variable SET max=(SELECT max(value) FROM value_2d WHERE var_id=35) WHERE uid=35;
-- for all other vars
-- min
UPDATE variable SET min=(SELECT min(value) FROM value_3d WHERE var_id=27) WHERE uid=27;
UPDATE variable SET min=(SELECT min(value) FROM value_3d WHERE var_id=29) WHERE uid=29;
UPDATE variable SET min=(SELECT min(value) FROM value_3d WHERE var_id=30) WHERE uid=30;
UPDATE variable SET min=(SELECT min(value) FROM value_3d WHERE var_id=31) WHERE uid=31;
UPDATE variable SET min=(SELECT min(value) FROM value_3d WHERE var_id=32) WHERE uid=32;
UPDATE variable SET min=(SELECT min(value) FROM value_3d WHERE var_id=33) WHERE uid=33;
UPDATE variable SET min=(SELECT min(value) FROM value_3d WHERE var_id=34) WHERE uid=34;
-- max
UPDATE variable SET max=(SELECT max(value) FROM value_3d WHERE var_id=27) WHERE uid=27;
UPDATE variable SET max=(SELECT max(value) FROM value_3d WHERE var_id=29) WHERE uid=29;
UPDATE variable SET max=(SELECT max(value) FROM value_3d WHERE var_id=30) WHERE uid=30;
UPDATE variable SET max=(SELECT max(value) FROM value_3d WHERE var_id=31) WHERE uid=31;
UPDATE variable SET max=(SELECT max(value) FROM value_3d WHERE var_id=32) WHERE uid=32;
UPDATE variable SET max=(SELECT max(value) FROM value_3d WHERE var_id=33) WHERE uid=33;
UPDATE variable SET max=(SELECT max(value) FROM value_3d WHERE var_id=34) WHERE uid=34;

-- update axes_mins/maxs, compute per axis, not per variable, otherwise redundant
-- compute min/max for T
-- SELECT uid,name FROM variable WHERE dataset_id=7 AND name='time'; // 28
-- SELECT min(value) FROM value_1d WHERE var_id=28; // 43829.0416667
-- SELECT max(value) FROM value_1d WHERE var_id=28; // 55151.0416667
-- compute min/max for Y
-- SELECT uid,name FROM variable WHERE dataset_id=7 AND name='lat'; // 25
-- SELECT min(value) FROM value_1d WHERE var_id=25; // 80.125
-- SELECT max(value) FROM value_1d WHERE var_id=25; // 83.875
-- compute min/max for X
-- SELECT uid,name FROM variable WHERE dataset_id=7 AND name='lon'; // 26
-- SELECT min(value) FROM value_1d WHERE var_id=26; // -87.875
-- SELECT max(value) FROM value_1d WHERE var_id=26; // -84.125
-- manually plugin these mins/maxs for the D vars
-- SELECT uid,name,axes FROM variable WHERE dataset_id=7 AND type='D' AND axes='{"T","Y","X"}'; // 27,29,30,31,32,33,34
UPDATE variable SET axes_mins='{43829.0416667,80.125,-87.875}',axes_maxs='{55151.0416667,83.875,-84.125}'
WHERE uid=ANY('{27,29,30,31,32,33,34}');
-- SELECT uid,name,axes FROM variable WHERE dataset_id=7 AND type='D' AND axes='{"Y","X"}'; // 35
UPDATE variable SET axes_mins='{80.125,-87.875}',axes_maxs='{83.875,-84.125}' WHERE uid=35;

-- delete redundant variables
DELETE FROM variable WHERE 8<=dataset_id AND dataset_id<=10;

COMMIT;


----------
-- GSDE --
----------

-- files

-- Ingest the following GSDE files in this order
-- (make sure to first add the postive=up attribute to the 'depth' var + make units of 'slal' var non-empty, say N/A):
-- soil_0013_0038.tile.nc4
-- soil_0013_0039.tile.nc4
-- soil_0014_0038.tile.nc4
-- soil_0014_0039.tile.nc4

-- correction commands, wrapped in a transaction

BEGIN;

-- delete from dataset
-- SELECT uid FROM dataset; // 1,7,11,12,13,14
DELETE FROM dataset WHERE 12<=uid AND uid<=14;

-- relink data in value_lat_lon
-- SELECT DISTINCT array_agg(uid) FROM variable WHERE 11<= dataset_id AND dataset_id<=14 AND axes='{"Y","X"}' GROUP BY name;
UPDATE value_lat_lon SET var_id=72 WHERE var_id=ANY('{104,136,168}');
UPDATE value_lat_lon SET var_id=73 WHERE var_id=ANY('{105,137,169}');
UPDATE value_lat_lon SET var_id=74 WHERE var_id=ANY('{106,138,170}');
UPDATE value_lat_lon SET var_id=75 WHERE var_id=ANY('{107,139,171}');
UPDATE value_lat_lon SET var_id=76 WHERE var_id=ANY('{108,140,172}');
UPDATE value_lat_lon SET var_id=77 WHERE var_id=ANY('{109,141,173}');
UPDATE value_lat_lon SET var_id=78 WHERE var_id=ANY('{110,142,174}');

-- relink data in value_vertical_lat_lon
-- use temporary index to be faster
-- SELECT uid,name FROM variable WHERE dataset_id=11 AND axes='{"Z","Y","X"}' ORDER BY uid;
-- SELECT uid,name FROM variable WHERE name='sbdm' ORDER BY uid; // 79,111,143,175
-- SELECT uid,name FROM variable WHERE name='scec' ORDER BY uid; // 80,112,144,176
-- SELECT uid,name FROM variable WHERE name='slec' ORDER BY uid; // 81,113,145,177
-- SELECT uid,name FROM variable WHERE name='slca' ORDER BY uid; // 82,114,146,178
-- SELECT uid,name FROM variable WHERE name='slal' ORDER BY uid; // ...
-- SELECT uid,name FROM variable WHERE name='slcf' ORDER BY uid; //
-- SELECT uid,name FROM variable WHERE name='slcl' ORDER BY uid; //
-- SELECT uid,name FROM variable WHERE name='slsi' ORDER BY uid; //
-- SELECT uid,name FROM variable WHERE name='caco3' ORDER BY uid; //
-- SELECT uid,name FROM variable WHERE name='ssat' ORDER BY uid; //
-- SELECT uid,name FROM variable WHERE name='sdul' ORDER BY uid; //
-- SELECT uid,name FROM variable WHERE name='slmg' ORDER BY uid; //
-- SELECT uid,name FROM variable WHERE name='sloc' ORDER BY uid; //
-- SELECT uid,name FROM variable WHERE name='slke' ORDER BY uid; //
-- SELECT uid,name FROM variable WHERE name='slbs' ORDER BY uid; //
-- SELECT uid,name FROM variable WHERE name='ssks' ORDER BY uid; //
-- SELECT uid,name FROM variable WHERE name='srgf' ORDER BY uid; //
-- SELECT uid,name FROM variable WHERE name='slhw' ORDER BY uid; //
-- SELECT uid,name FROM variable WHERE name='slll' ORDER BY uid; //
-- SELECT uid,name FROM variable WHERE name='slna' ORDER BY uid; //
-- SELECT uid,name FROM variable WHERE name='slpt' ORDER BY uid; // ...
-- SELECT uid,name FROM variable WHERE name='slni' ORDER BY uid; // 100,132,164,196
CREATE index value_vertical_lat_lon_var_id_idx ON value_vertical_lat_lon(var_id);
UPDATE value_vertical_lat_lon SET var_id=79 WHERE var_id=ANY('{111,143,175}');
UPDATE value_vertical_lat_lon SET var_id=80 WHERE var_id=ANY('{112,144,176}');
UPDATE value_vertical_lat_lon SET var_id=81 WHERE var_id=ANY('{113,145,177}');
UPDATE value_vertical_lat_lon SET var_id=82 WHERE var_id=ANY('{114,146,178}');
UPDATE value_vertical_lat_lon SET var_id=83 WHERE var_id=ANY('{115,147,179}');
UPDATE value_vertical_lat_lon SET var_id=84 WHERE var_id=ANY('{116,148,180}');
UPDATE value_vertical_lat_lon SET var_id=85 WHERE var_id=ANY('{117,149,181}');
UPDATE value_vertical_lat_lon SET var_id=86 WHERE var_id=ANY('{118,150,182}');
UPDATE value_vertical_lat_lon SET var_id=87 WHERE var_id=ANY('{119,151,183}');
UPDATE value_vertical_lat_lon SET var_id=88 WHERE var_id=ANY('{120,152,184}');
UPDATE value_vertical_lat_lon SET var_id=89 WHERE var_id=ANY('{121,153,185}');
UPDATE value_vertical_lat_lon SET var_id=90 WHERE var_id=ANY('{122,154,186}');
UPDATE value_vertical_lat_lon SET var_id=91 WHERE var_id=ANY('{123,155,187}');
UPDATE value_vertical_lat_lon SET var_id=92 WHERE var_id=ANY('{124,156,188}');
UPDATE value_vertical_lat_lon SET var_id=93 WHERE var_id=ANY('{125,157,189}');
UPDATE value_vertical_lat_lon SET var_id=94 WHERE var_id=ANY('{126,158,190}');
UPDATE value_vertical_lat_lon SET var_id=95 WHERE var_id=ANY('{127,159,191}');
UPDATE value_vertical_lat_lon SET var_id=96 WHERE var_id=ANY('{128,160,192}');
UPDATE value_vertical_lat_lon SET var_id=97 WHERE var_id=ANY('{129,161,193}');
UPDATE value_vertical_lat_lon SET var_id=98 WHERE var_id=ANY('{130,162,194}');
UPDATE value_vertical_lat_lon SET var_id=99 WHERE var_id=ANY('{131,163,195}');
UPDATE value_vertical_lat_lon SET var_id=100 WHERE var_id=ANY('{132,164,196}');
DROP index value_vertical_lat_lon_var_id_idx;

-- relink data in value_1d: depth(depth),lat(lat),lon(lon)

-- depth(depth)
-- SELECT uid,name FROM variable WHERE 11<=dataset_id AND dataset_id<=14 AND name='depth'; // 71,103,135,167
DELETE FROM value_1d WHERE var_id=ANY('{103,135,167}');

-- lat(lat)
-- SELECT uid,name FROM variable WHERE dataset_id=ANY('{12,14}') AND name='lat'; // 101,165
DELETE FROM value_1d WHERE var_id=ANY('{101,165}');
-- SELECT uid,name FROM variable WHERE dataset_id=13 AND name='lat'; // 133
UPDATE value_1d SET index_0=index_0+8 WHERE var_id=133;
-- SELECT uid,name FROM variable WHERE dataset_id=11 AND name='lat'; // 69
UPDATE value_1d SET var_id=69 WHERE var_id=133;

-- lon(lon)
-- SELECT uid,name FROM variable WHERE dataset_id=ANY('{13,14}') AND name='lon'; // 134,166
DELETE FROM value_1d WHERE var_id=ANY('{134,166}');
-- SELECT uid,name FROM variable WHERE dataset_id=12 AND name='lon'; // 102
UPDATE value_1d SET index_0=index_0+8 WHERE var_id=102;
-- SELECT uid,name FROM variable WHERE dataset_id=11 AND name='lon'; // 70
UPDATE value_1d SET var_id=70 WHERE var_id=102;

-- relink data in value_2d: var(lat,lon)
-- SELECT DISTINCT array_agg(uid) FROM variable WHERE 11<= dataset_id AND dataset_id<=14 AND num_dims=2 GROUP BY name;
UPDATE value_2d SET index_1=index_1+8
WHERE var_id=ANY('{104,105,106,107,108,109,110}');
UPDATE value_2d SET index_0=index_0+8
WHERE var_id=ANY('{136,137,138,139,140,141,142}');
UPDATE value_2d SET index_0=index_0+8, index_1=index_1+8
WHERE var_id=ANY('{168,169,170,171,172,173,174}');
UPDATE value_2d SET var_id=72 WHERE var_id=ANY('{104,136,168}');
UPDATE value_2d SET var_id=73 WHERE var_id=ANY('{105,137,169}');
UPDATE value_2d SET var_id=74 WHERE var_id=ANY('{106,138,170}');
UPDATE value_2d SET var_id=75 WHERE var_id=ANY('{107,139,171}');
UPDATE value_2d SET var_id=76 WHERE var_id=ANY('{108,140,172}');
UPDATE value_2d SET var_id=77 WHERE var_id=ANY('{109,141,173}');
UPDATE value_2d SET var_id=78 WHERE var_id=ANY('{110,142,174}');

-- relink data in value_3d: var(depth,lat,lon)
-- use temporary index to be faster
-- update lat,lon indexes
-- SELECT DISTINCT array_agg(uid) FROM variable WHERE 11<= dataset_id AND dataset_id<=14 AND num_dims=3 GROUP BY name;
CREATE index value_3d_var_id_idx ON value_3d(var_id);
UPDATE value_3d SET index_2=index_2+8
WHERE var_id=ANY('{111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,
126,127,128,129,130,131,132}');
UPDATE value_3d SET index_1=index_1+8
WHERE var_id=ANY('{143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,
158,159,160,161,162,163,164}');
UPDATE value_3d SET index_1=index_1+8, index_2=index_2+8
WHERE var_id=ANY('{175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,
190,191,192,193,194,195,196}');
-- relink var_id to point to representative
UPDATE value_3d SET var_id=79 WHERE var_id=ANY('{111,143,175}');
UPDATE value_3d SET var_id=80 WHERE var_id=ANY('{112,144,176}');
UPDATE value_3d SET var_id=81 WHERE var_id=ANY('{113,145,177}');
UPDATE value_3d SET var_id=82 WHERE var_id=ANY('{114,146,178}');
UPDATE value_3d SET var_id=83 WHERE var_id=ANY('{115,147,179}');
UPDATE value_3d SET var_id=84 WHERE var_id=ANY('{116,148,180}');
UPDATE value_3d SET var_id=85 WHERE var_id=ANY('{117,149,181}');
UPDATE value_3d SET var_id=86 WHERE var_id=ANY('{118,150,182}');
UPDATE value_3d SET var_id=87 WHERE var_id=ANY('{119,151,183}');
UPDATE value_3d SET var_id=88 WHERE var_id=ANY('{120,152,184}');
UPDATE value_3d SET var_id=89 WHERE var_id=ANY('{121,153,185}');
UPDATE value_3d SET var_id=90 WHERE var_id=ANY('{122,154,186}');
UPDATE value_3d SET var_id=91 WHERE var_id=ANY('{123,155,187}');
UPDATE value_3d SET var_id=92 WHERE var_id=ANY('{124,156,188}');
UPDATE value_3d SET var_id=93 WHERE var_id=ANY('{125,157,189}');
UPDATE value_3d SET var_id=94 WHERE var_id=ANY('{126,158,190}');
UPDATE value_3d SET var_id=95 WHERE var_id=ANY('{127,159,191}');
UPDATE value_3d SET var_id=96 WHERE var_id=ANY('{128,160,192}');
UPDATE value_3d SET var_id=97 WHERE var_id=ANY('{129,161,193}');
UPDATE value_3d SET var_id=98 WHERE var_id=ANY('{130,162,194}');
UPDATE value_3d SET var_id=99 WHERE var_id=ANY('{131,163,195}');
UPDATE value_3d SET var_id=100 WHERE var_id=ANY('{132,164,196}');
DROP index value_3d_var_id_idx;

-- recompute min,max info + some other metadata for vars

-- update dims_sizes
-- SELECT uid,name,dims_sizes FROM variable WHERE dataset_id=11;
UPDATE variable SET dims_sizes='{480}' WHERE dataset_id=11 AND name='lat';
UPDATE variable SET dims_sizes='{480}' WHERE dataset_id=11 AND name='lon';
-- SELECT uid,name FROM variable WHERE dataset_id=11 AND axes='{"Y","X"}';
UPDATE variable SET dims_sizes='{480,480}' WHERE dataset_id=11
AND axes='{"Y","X"}';
-- SELECT uid,name FROM variable WHERE dataset_id=11 AND axes='{"Z","Y","X"}';
UPDATE variable SET dims_sizes='{8,480,480}' WHERE dataset_id=11
AND axes='{"Z","Y","X"}';

-- update min,max
-- for lat
-- SELECT uid,name,min,max FROM variable WHERE 11<=dataset_id AND dataset_id<=14 AND name='lat'; // 69,101,133,165
UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{69,101,133,165}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{69,101,133,165}')
)
WHERE uid=69;

-- for lon
-- SELECT uid,name,min,max FROM variable WHERE 11<=dataset_id AND dataset_id<=14 AND name='lon'; // 70,102,134,166
UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{70,102,134,166}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{70,102,134,166}')
)
WHERE uid=70;

-- for 2d vars
UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{72,104,136,168}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{72,104,136,168}')
)
WHERE uid=72;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{73,105,137,169}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{73,105,137,169}')
)
WHERE uid=73;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{74,106,138,170}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{74,106,138,170}')
)
WHERE uid=74;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{75,107,139,171}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{75,107,139,171}')
)
WHERE uid=75;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{76,108,140,172}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{76,108,140,172}')
)
WHERE uid=76;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{77,109,141,173}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{77,109,141,173}')
)
WHERE uid=77;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{78,110,142,174}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{78,110,142,174}')
)
WHERE uid=78;

-- for 3d vars
UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{79,111,143,175}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{79,111,143,175}')
)
WHERE uid=79;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{80,112,144,176}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{80,112,144,176}')
)
WHERE uid=80;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{81,113,145,177}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{81,113,145,177}')
)
WHERE uid=81;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{82,114,146,178}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{82,114,146,178}')
)
WHERE uid=82;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{83,115,147,179}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{83,115,147,179}')
)
WHERE uid=83;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{84,116,148,180}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{84,116,148,180}')
)
WHERE uid=84;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{85,117,149,181}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{85,117,149,181}')
)
WHERE uid=85;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{86,118,150,182}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{86,118,150,182}')
)
WHERE uid=86;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{87,119,151,183}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{87,119,151,183}')
)
WHERE uid=87;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{88,120,152,184}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{88,120,152,184}')
)
WHERE uid=88;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{89,121,153,185}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{89,121,153,185}')
)
WHERE uid=89;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{90,122,154,186}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{90,122,154,186}')
)
WHERE uid=90;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{91,123,155,187}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{91,123,155,187}')
)
WHERE uid=91;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{92,124,156,188}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{92,124,156,188}')
)
WHERE uid=92;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{93,125,157,189}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{93,125,157,189}')
)
WHERE uid=93;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{94,126,158,190}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{94,126,158,190}')
)
WHERE uid=94;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{95,127,159,191}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{95,127,159,191}')
)
WHERE uid=95;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{96,128,160,192}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{96,128,160,192}')
)
WHERE uid=96;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{97,129,161,193}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{97,129,161,193}')
)
WHERE uid=97;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{98,130,162,194}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{98,130,162,194}')
)
WHERE uid=98;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{99,131,163,195}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{99,131,163,195}')
)
WHERE uid=99;

UPDATE variable SET min=
(
SELECT min(min) FROM variable
WHERE uid=ANY('{100,132,164,196}')
),
max=
(
SELECT max(max) FROM variable
WHERE uid=ANY('{100,132,164,196}')
)
WHERE uid=100;

-- update axes_mins/maxs, compute per axis, not per variable, otherwise redundant
-- compute min/max for Z
-- SELECT uid,name FROM variable WHERE dataset_id=11 AND name='depth'; // 71
-- SELECT min(value) FROM value_1d WHERE var_id=71; // 4.5
-- SELECT max(value) FROM value_1d WHERE var_id=71; // 229.600006104
-- compute min/max for Y
-- SELECT uid,name FROM variable WHERE dataset_id=11 AND name='lat'; // 69
-- SELECT min(value) FROM value_1d WHERE var_id=69; // 62.004119873
-- SELECT max(value) FROM value_1d WHERE var_id=69; // 65.9957809448
-- compute min/max for X
-- SELECT uid,name FROM variable WHERE dataset_id=11 AND name='lon'; // 70
-- SELECT min(value) FROM value_1d WHERE var_id=70; // -105.995864868
-- SELECT max(value) FROM value_1d WHERE var_id=70; // -102.004196167
-- manually plugin these mins/maxs for the D vars
-- SELECT uid,name,axes FROM variable WHERE dataset_id=11 AND type='D' AND axes='{"Z","Y","X"}';
UPDATE variable SET axes_mins='{4.5,62.004119873,-105.995864868}',
axes_maxs='{229.600006104,65.9957809448,-102.004196167}'
WHERE dataset_id=11 AND type='D' AND axes='{"Z","Y","X"}';
-- SELECT uid,name,axes FROM variable WHERE dataset_id=11 AND type='D' AND axes='{"Y","X"}';
UPDATE variable SET axes_mins='{62.004119873,-105.995864868}',
axes_maxs='{65.9957809448,-102.004196167}'
WHERE dataset_id=11 AND type='D' AND axes='{"Y","X"}';

-- delete redundant variables
DELETE FROM variable WHERE 12<=dataset_id AND dataset_id<=14;

COMMIT;