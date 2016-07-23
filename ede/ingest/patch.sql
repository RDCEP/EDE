-- Correct raster_datasets table
DELETE FROM raster_datasets WHERE uid>1;

-- Correct dataset_id field for raster_variables table
UPDATE raster_variables SET dataset_id=1;

-- Correct raster_data_single table
UPDATE raster_data_single SET dataset_id=1;