CREATE TABLE dataset (
  uid bigserial primary key,
  attrs jsonb
);

CREATE TABLE variable (
  uid bigserial primary key,
  dataset_id integer,
  name text,
  datatype text,
  num_dims integer,
  dims_names text[],
  dims_sizes integer[],
  attrs jsonb,
  min double precision,
  max double precision,
  type char(1),
  axes char(1)[],
  axes_mins double precision[],
  axes_maxs double precision[],
  axes_units text[]
);

CREATE TABLE value_1d (
  var_id integer,
  index_0 integer,
  value_0 double precision,
  value double precision
);

CREATE TABLE value_2d (
  var_id integer,
  index_0 integer,
  value_0 double precision,
  index_1 integer,
  value_1 double precision,
  value double precision
);

CREATE TABLE value_3d (
  var_id integer,
  index_0 integer,
  value_0 double precision,
  index_1 integer,
  value_1 double precision,
  index_2 integer,
  value_2 double precision,
  value double precision
);

CREATE TABLE value_4d (
  var_id integer,
  index_0 integer,
  value_0 double precision,
  index_1 integer,
  value_1 double precision,
  index_2 integer,
  value_2 double precision,
  index_3 integer,
  value_3 double precision,
  value double precision
);

CREATE TABLE value_time (
  var_id integer,
  time_value double precision,
  time_stamp timestamp with time zone,
  value double precision
);

CREATE TABLE value_vertical (
  var_id integer,
  vertical_value double precision,
  value double precision
);

CREATE TABLE value_lat_lon (
  var_id integer,
  geom geometry(Point,4326),
  value double precision
);

CREATE TABLE value_time_lat_lon (
  var_id integer,
  time_value double precision,
  time_stamp timestamp with time zone,
  geom geometry(Point,4326),
  value double precision
);

CREATE TABLE value_vertical_lat_lon (
  var_id integer,
  vertical_value double precision,
  geom geometry(Point,4326),
  value double precision
);

CREATE TABLE value_time_vertical_lat_lon (
  var_id integer,
  time_value double precision,
  time_stamp timestamp with time zone,
  vertical_value double precision,
  geom geometry(Point,4326),
  value double precision
);

CREATE TABLE regionsets (
  uid bigserial primary key,
  name text,
  attrs text[]
);

CREATE TABLE regions (
  uid bigserial primary key,
  regionset_id integer,
  geom geometry(Geometry,4326),
  attrs jsonb
);