CREATE OR REPLACE FUNCTION get_raster_values(mid int, vid int)
RETURNS TABLE(nband int, valarray double precision[][]) AS $$
BEGIN
  RETURN QUERY
  SELECT (ST_DumpValues(r.rast)).*
  FROM grid_data AS r
  WHERE meta_id=mid AND var_id=vid;
END;
$$ LANGUAGE plpgsql;