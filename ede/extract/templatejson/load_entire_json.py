#!/usr/bin/env python

import json

with open('template.json') as f:
	
	data = json.load(f)
	
	for pt in data['response']['data']:
		pt['properties'] = {'values': [3.115]}
	
	with open('template.json.out', 'w') as g:
		json.dump(data, g)
