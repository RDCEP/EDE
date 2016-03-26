CREATE OR REPLACE FUNCTION polygon_from_bbox(
  north FLOAT, east FLOAT, south FLOAT, west FLOAT
) RETURNS GEOMETRY AS $$
BEGIN
  RETURN ST_Polygon(ST_GeomFromText('LINESTRING('
                                    || west || ' ' || south || ', '
                                    || east || ' ' || south || ', '
                                    || east || ' ' || north || ', '
                                    || west || ' ' || north || ', '
                                    || west || ' ' || south || ')'), 4326);
END;
$$ LANGUAGE plpgsql;