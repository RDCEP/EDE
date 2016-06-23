#!/usr/bin/env python

from ijson import parse

f = open('template.json', 'r')
g = open('out.json', 'w')

parser = parse(f)
for prefix, event, value in parser:
	if prefix == 'response.data.item.properties' and event == 'start_map':
		g.write('[3.1415]\n')
