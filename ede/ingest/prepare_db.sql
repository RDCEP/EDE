-- Create foreign keys
ALTER TABLE variable ADD CONSTRAINT variable_dataset_id_dataset_fk FOREIGN KEY (dataset_id)
REFERENCES dataset ON DELETE CASCADE;

ALTER TABLE value_1d ADD CONSTRAINT value_1d_var_id_variable_fk FOREIGN KEY (var_id)
REFERENCES variable ON DELETE CASCADE;

ALTER TABLE value_2d ADD CONSTRAINT value_2d_var_id_variable_fk FOREIGN KEY (var_id)
REFERENCES variable ON DELETE CASCADE;

ALTER TABLE value_3d ADD CONSTRAINT value_3d_var_id_variable_fk FOREIGN KEY (var_id)
REFERENCES variable ON DELETE CASCADE;

ALTER TABLE value_4d ADD CONSTRAINT value_4d_var_id_variable_fk FOREIGN KEY (var_id)
REFERENCES variable ON DELETE CASCADE;

ALTER TABLE value_time ADD CONSTRAINT value_time_var_id_variable_fk
FOREIGN KEY (var_id) REFERENCES variable ON DELETE CASCADE;

ALTER TABLE value_vertical ADD CONSTRAINT value_vertical_var_id_variable_fk
FOREIGN KEY (var_id) REFERENCES variable ON DELETE CASCADE;

ALTER TABLE value_lat_lon ADD CONSTRAINT value_lat_lon_var_id_variable_fk
FOREIGN KEY (var_id) REFERENCES variable ON DELETE CASCADE;

ALTER TABLE value_time_lat_lon ADD CONSTRAINT value_time_lat_lon_var_id_variable_fk
FOREIGN KEY (var_id) REFERENCES variable ON DELETE CASCADE;

ALTER TABLE value_vertical_lat_lon ADD CONSTRAINT value_vertical_lat_lon_var_id_variable_fk
FOREIGN KEY (var_id) REFERENCES variable ON DELETE CASCADE;

ALTER TABLE value_time_vertical_lat_lon ADD CONSTRAINT value_time_vertical_lat_lon_var_id_variable_fk
FOREIGN KEY (var_id) REFERENCES variable ON DELETE CASCADE;

-- Create indexes
-- This extension is needed for composite indexes that involve a geom and other fields
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- On value_1d
DROP INDEX IF EXISTS value_1d_var_id_index_0_idx;
DROP INDEX IF EXISTS value_1d_var_id_value_0_idx;
CREATE INDEX value_1d_var_id_index_0_idx ON value_1d(var_id, index_0);
CREATE INDEX value_1d_var_id_value_0_idx ON value_1d(var_id, value_0);

-- On value_2d
DROP INDEX IF EXISTS value_2d_var_id_index_0_index_1_idx;
DROP INDEX IF EXISTS value_2d_var_id_value_0_value_1_idx;
CREATE INDEX value_2d_var_id_index_0_index_1_idx ON value_2d(var_id, index_0, index_1);
CREATE INDEX value_2d_var_id_value_0_value_1_idx ON value_2d(var_id, value_0, value_1);

-- On value_3d
DROP INDEX IF EXISTS value_3d_var_id_index_0_index_1_index_2_idx;
DROP INDEX IF EXISTS value_3d_var_id_value_0_value_1_value_2_idx;
CREATE INDEX value_3d_var_id_index_0_index_1_index_2_idx ON value_3d(var_id, index_0, index_1, index_2);
CREATE INDEX value_3d_var_id_value_0_value_1_value_2_idx ON value_3d(var_id, value_0, value_1, value_2);

-- On value_4d
DROP INDEX IF EXISTS value_4d_var_id_index_0_index_1_index_2_index_3_idx;
DROP INDEX IF EXISTS value_4d_var_id_value_0_value_1_value_2_value_3_idx;
CREATE INDEX value_4d_var_id_index_0_index_1_index_2_index_3_idx
ON value_4d(var_id, index_0, index_1, index_2, index_3);
CREATE INDEX value_4d_var_id_value_0_value_1_value_2_value_3_idx
ON value_4d(var_id, value_0, value_1, value_2, value_3);

-- On value_time
DROP INDEX IF EXISTS value_time_var_id_time_value_idx;
DROP INDEX IF EXISTS value_time_var_id_time_stamp_idx;
CREATE INDEX value_time_var_id_time_value_idx ON value_time(var_id, time_value);
CREATE INDEX value_time_var_id_time_stamp_idx ON value_time(var_id, time_stamp);

-- On value_vertical
DROP INDEX IF EXISTS value_vertical_var_id_vertical_value_idx;
CREATE INDEX value_vertical_var_id_vertical_value_idx ON value_vertical(var_id, vertical_value);

-- On value_lat_lon
DROP INDEX IF EXISTS value_lat_lon_var_id_geom_idx;
CREATE INDEX value_lat_lon_var_id_geom_idx ON value_lat_lon USING GIST (var_id, geom);

-- On value_time_lat_lon
DROP INDEX IF EXISTS value_time_lat_lon_var_id_geom_time_stamp_idx;
DROP INDEX IF EXISTS value_time_lat_lon_var_id_geom_time_value_idx;
CREATE INDEX value_time_lat_lon_var_id_geom_time_stamp_idx ON value_time_lat_lon USING GIST (var_id, geom, time_stamp);
CREATE INDEX value_time_lat_lon_var_id_geom_time_value_idx ON value_time_lat_lon USING GIST (var_id, geom, time_value);

-- On value_vertical_lat_lon
DROP INDEX IF EXISTS value_vertical_lat_lon_var_id_geom_vertical_value_idx;
CREATE INDEX value_vertical_lat_lon_var_id_geom_vertical_value_idx ON value_vertical_lat_lon
USING GIST (var_id, geom, vertical_value);

-- On value_time_vertical_lat_lon
DROP INDEX IF EXISTS value_time_vertical_lat_lon_var_id_geom_time_value_vertical_value_idx;
DROP INDEX IF EXISTS value_time_vertical_lat_lon_var_id_geom_time_stamp_vertical_value_idx;
CREATE INDEX value_time_vertical_lat_lon_var_id_geom_time_value_vertical_value_idx ON value_time_vertical_lat_lon
USING GIST (var_id, geom, time_value, vertical_value);
CREATE INDEX value_time_vertical_lat_lon_var_id_geom_time_stamp_vertical_value_idx ON value_time_vertical_lat_lon
USING GIST (var_id, geom, time_stamp, vertical_value);