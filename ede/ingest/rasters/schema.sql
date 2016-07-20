CREATE TABLE raster_datasets (
  uid bigserial primary key,
  short_name text,
  long_name text,
  lon_start double precision,
  lon_end double precision,
  lon_step double precision,
  num_lons integer,
  lat_start double precision,
  lat_end double precision,
  lat_step double precision,
  num_lats integer,
  bbox geometry(Polygon,4326),
  time_start timestamp with time zone,
  time_end timestamp with time zone,
  time_step interval,
  num_times integer,
  time_unit text,
  attrs jsonb
);

CREATE TABLE raster_variables (
  uid bigserial primary key,
  dataset_id integer,
  name text,
  attrs jsonb
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

CREATE TABLE raster_data_single (
  uid bigserial primary key,
  dataset_id integer,
  var_id integer,
  geom geometry(Point,4326),
  time_id integer,
  value double precision
);

CREATE TABLE raster_data_series (
  uid bigserial primary key,
  dataset_id integer,
  var_id integer,
  geom geometry(Point,4326),
  values double precision[]
);