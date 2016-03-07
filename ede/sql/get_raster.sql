CREATE TYPE ede_pixel AS (
  x FLOAT,
  y FLOAT,
  val DOUBLE PRECISION[]
);

CREATE OR REPLACE FUNCTION get_raster(mid int, vid int)
RETURNS SETOF ede_pixel AS $$

$$ LANGUAGE plpythonu;