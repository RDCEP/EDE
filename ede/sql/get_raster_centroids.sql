CREATE TYPE raster_centroid AS (
  x INT,
  y INT,
  st_astext TEXT
);

CREATE OR REPLACE FUNCTION get_raster_centroids(
  mid INT, vid INT, north FLOAT, east FLOAT, south FLOAT, west FLOAT)
RETURNS SETOF raster_centroid AS $$
DECLARE
  poly GEOMETRY;
BEGIN
  poly := polygon_from_bbox(north, east, south, west);
  RETURN QUERY
  SELECT r.x, r.y, ST_AsText(r.geom)
  FROM (SELECT (ST_PixelAsCentroids(rast, 1, FALSE)).*
        FROM grid_data AS gd
        WHERE gd.meta_id = mid
              AND gd.var_id = vid
              AND ST_Intersects(
                rast, poly)
       ) AS r;
END;
$$ LANGUAGE plpgsql;
