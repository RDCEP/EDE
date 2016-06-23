#!/usr/bin/env python 

import json

num_lons = 720
num_lats = 360
lon_start = -179.75
lon_step = 0.5
lat_start = 89.75
lat_step = -0.5

with open('template.json', 'w') as outfile:
	res = {}

	res['request'] = {}
	res['request']['url'] = '/api/v0/griddata/dataset/1/var/1/time/1'
	
	res['response'] = {}
	res['response']['status'] = 'OK'
	res['response']['status_code'] = 200
	
	res['response']['metadata'] = {}
	res['response']['metadata']['format'] = 'grid'
	res['response']['region'] = {}
	res['response']['region']['type'] = 'Polygon'
	res['response']['region']['coordinates'] = [[ [-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90] ]]
		
	res['response']['data'] = []
	lat_c = lat_start
	for i in range(num_lats):
		lon_c = lon_start
		for j in range(num_lons):
			new_pt = {}
			new_pt['type'] = 'Feature'
			new_pt['geometry'] = {'type': 'Point', 'coordinates': [lon_c, lat_c]}
			new_pt['properties'] = {}
			res['response']['data'].append(new_pt)
			lon_c += lon_step
		lat_c += lat_step
	json.dump(res, outfile)
