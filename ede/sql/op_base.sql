-- Aggregation base functions for EDE
-- __author__ = rlourenc@mail.depaul.edu
-- TODO : Implement op_base.py functions in SQL
----------------------------------------------
-- Transaction Codes:
-- 01 - Aggregation by mean
-- 02 - Binning
-- 03 - Normalization
----------------------------------------------
-- Define return type
CREATE TYPE raster_values AS (
  nband INT,
  valarray FLOAT[][]
);

-- Run base function
CREATE OR REPLACE FUNCTION op_base(code INT, mid INT, vid INT, ts TIMESTAMP, north FLOAT, east FLOAT, south FLOAT, west FLOAT)
 RETURNS SETOF raster_values AS $$

DECLARE
  poly GEOMETRY;

BEGIN
 -- logic

-- Create bounding box polygon from coordinates
poly := polygon_from_bbox(north, east, south, west);

-- Define transactions

-- Transaction 01 - Aggregation by mean
IF code = 01 THEN
  if-statement;
-- TODO: Implements Aggregation by mean (Lack Validation)

      RETURN QUERY

      SELECT DISTINCT
        (ST_PixelAsCentroids(rast)).x,
        (ST_PixelAsCentroids(rast)).y,
        AVG((ST_PixelAsCentroids(rast)).val)
        OVER (PARTITION BY (ST_PixelAsCentroids(rast)).x, (ST_PixelAsCentroids(rast)).y)
        AS "Avg_Val"

			FROM grid_data
			WHERE meta_id = mid AND var_id = vid AND --time = '1980-01-01 00:00:00-06' AND
			      ST_Intersects(rast,poly)

			GROUP BY (ST_PixelAsCentroids(rast)).x,
				 (ST_PixelAsCentroids(rast)).y,
				 ST_PixelAsCentroids(rast);

-- Transaction 02 - Binning
ELSIF code = 02 THEN
  elsif-statement-2;
-- TODO: Implements Binning

-- Transaction 03 - Normalization
ELSIF code = 03 THEN
  elsif-statement-n;
-- TODO: Implements Normalization

ELSE
  else-statement;
-- TODO: Throw exception code

END IF:


-- End logic
END;
LANGUAGE plpgsql;