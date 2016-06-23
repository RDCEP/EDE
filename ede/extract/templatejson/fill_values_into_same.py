#!/usr/bin/env python

from ijson import parse

f = open('template.json', 'r+')

parser = parse(f)
for prefix, event, value in parser:
	if prefix == 'response.data.item.properties' and event == 'start_map':
		print(value)
		#f.write('[3.1415]')
