#!/usr/bin/env python

from ijson import parse

f = open('template.json', 'r')
g = open('out.json', 'w')

parser = parse(f)
for prefix, event, value in parser:
	#print("prefix: {}, value: {}, format: {}".format(prefix, value, format))
	if prefix == 'response.data.item.properties.values' and event == 'start_map':
		#print(value)
		g.write('[3.1415]\n')
