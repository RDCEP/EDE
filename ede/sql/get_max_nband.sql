
CREATE OR REPLACE FUNCTION get_max_nband(
  mid INT, vid INT, north FLOAT, east FLOAT, south FLOAT, west FLOAT)
RETURNS INT AS $$
DECLARE
  max_nband INT;
BEGIN
  SELECT INTO max_nband MAX(nband)
    FROM get_raster_values(mid, vid, north, east, south, west);
  RETURN max_nband;
END;
$$ LANGUAGE plpgsql;
