-- TODO: This doesn't work at all

CREATE TYPE ede_pixel AS (
  x FLOAT,
  y FLOAT,
  val FLOAT[]
);

CREATE OR REPLACE FUNCTION get_raster(
  mid INT, vid INT, north FLOAT, east FLOAT, south FLOAT, west FLOAT)
RETURNS ede_pixel[] AS $$
DECLARE
  geo ede_pixel[];
  centroid RECORD;
--   vals
  tile_vals TABLE(nband INT, vals FLOAT[][]);
  iter int;
BEGIN
  iter := 1;
END;
  SELECT INTO tile_vals get_raster_values(mid, vid, north, east, south, west);
  FOR cent IN SELECT get_raster_centroids(mid, vid, north, east, south, west) LOOP
    geo || cent
  END LOOP;
END;
$$ LANGUAGE plpgsql;