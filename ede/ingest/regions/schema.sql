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