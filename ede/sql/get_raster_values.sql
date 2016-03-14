CREATE TYPE raster_values AS (
  nband INT,
  valarray FLOAT[][]
);


CREATE OR REPLACE FUNCTION get_raster_values(
  mid INT, vid INT, north FLOAT, east FLOAT, south FLOAT, west FLOAT)
RETURNS SETOF raster_values AS $$
DECLARE
  poly GEOMETRY;
BEGIN
  poly := polygon_from_bbox(north, east, south, west);
  RETURN QUERY
  SELECT (ST_DumpValues(r.rast)).*
  FROM grid_data AS r
  WHERE meta_id=mid
        AND var_id=vid
        AND ST_Intersects(
          rast, poly);
END;
$$ LANGUAGE plpgsql;