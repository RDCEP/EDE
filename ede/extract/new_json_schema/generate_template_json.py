#!/usr/bin/env python 

import json

num_lons = 720
num_lats = 360
lon_start = -179.75
lon_step = 0.5
lat_start = 89.75
lat_step = -0.5

with open('template.json', 'w') as outfile:
	
	lat_c = lat_start
	for i in range(num_lats):
		lon_c = lon_start
		for j in range(num_lons):
			new_pt = {}
			new_pt['type'] = 'Feature'
			new_pt['geometry'] = {'type': 'Point', 'coordinates': [lon_c, lat_c]}
			new_pt['properties'] = {'values': [3.1415]}
			outfile.write("INSERT INTO books(data) VALUES (\'")
			json.dump(new_pt, outfile)
			outfile.write('\');\n')
			lon_c += lon_step
		lat_c += lat_step
