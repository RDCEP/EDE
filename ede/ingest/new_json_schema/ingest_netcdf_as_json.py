#!/usr/bin/env python

from netCDF4 import Dataset
import numpy as np
import sys
import json

dataset_id = 1
filename = sys.argv[1]
#filename = '/data/psims/papsim_wfdei.cru_hist_fullharm_firr_yield_whe_annual_1979_2012.nc4'
var_id = int(sys.argv[2])
#var_id = 1

fh = Dataset(filename)

lons = fh.variables['lon'][:]
lats = fh.variables['lat'][:]

var_name = None
for var in fh.variables.values():
	if var.name not in ['lat','lon','time']:
		var_name = var.name

values = fh.variables[var_name][:]

outfile = open('out_file.sql', 'w')

for slice_id, slice in enumerate(values):
	time_id = slice_id+1
	for (lat_id, lon_id), value_tmp in np.ndenumerate(slice):
		value = float(value_tmp)
		if value_tmp == slice.get_fill_value():
			value = None
		lat = lats[lat_id]
		lon = lons[lon_id]
		json_data = {'type': 'Feature', 'geometry': {'type': 'Point', 'coordinates': [lon, lat]}, 'properties': {'values': [value]}}
		outfile.write('INSERT INTO grid_data(dataset_id, var_id, time_id, geom, json) VALUES ({}, {}, {}, ST_GeomFromText(\'POINT({} {})\', 4326), \'{}\');\n'.format(dataset_id, var_id, time_id, lon, lat, json.dumps(json_data)))
