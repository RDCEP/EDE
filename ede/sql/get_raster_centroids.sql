CREATE OR REPLACE FUNCTION get_raster_centroids(mid int, vid int, band int)
RETURNS TABLE(x int, y int, st_astext text) AS $$
BEGIN
  RETURN QUERY
  SELECT r.x, r.y, ST_AsText(r.geom)
  FROM (SELECT (ST_PixelAsCentroids(rast, band)).*
        FROM grid_data AS gd
        WHERE gd.meta_id = mid
              AND gd.var_id = vid
       ) AS r;
END;
$$ LANGUAGE plpgsql;