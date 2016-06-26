#!/usr/bin/env python

from netCDF4 import Dataset
import numpy as np
import sys
import json

dataset_id = 1
filename = sys.argv[1]
var_id = int(sys.argv[2])

fh = Dataset(filename)

lons = fh.variables['lon'][:]
lats = fh.variables['lat'][:]

var_name = None
for var in fh.variables.values():
	if var.name not in ['lat','lon','time']:
		var_name = var.name

values = fh.variables[var_name][:]

with open('out.csv', 'w') as f:
	for slice_id, slice in enumerate(values):
		time_id = slice_id+1
		for (lat_id, lon_id), value_tmp in np.ndenumerate(slice):
			value = float(value_tmp)
			if value_tmp == slice.get_fill_value():
				value = "null"
			lat = lats[lat_id]
			lon = lons[lon_id]
			json_data = "{{\"type\": \"Feature\", \"geometry\": {{\"type\": \"Point\", \"coordinates\": [{0}, {1}]}}, \"properties\": {{\"values\": [{2}]}}}}".format(lon, lat, value)
			f.write("{}\t{}\t{}\tSRID=4326;POINT({} {})\t{}\n".format(dataset_id, var_id, time_id, lon, lat, json_data))
