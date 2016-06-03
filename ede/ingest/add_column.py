import os, sys, subprocess, time
from netCDF4 import Dataset
from osgeo import gdal
import psycopg2
from psycopg2.extras import Json
import re
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST
from datetime import datetime, timedelta

def main(dates_filename, out_file):
    
	with open(dates_filename, 'r') as fin:
		content = fin.readlines()
		with open(out_file, 'w') as fout:
			for i, line in enumerate(content):
				line = line.strip(' \t\n\r')
				query = "update grid_dates set date_id={} where meta_id>=113 and date='{}';".format(i+1, line)
				fout.write(query + '\n')
				
if __name__ == "__main__":
	dates_file = sys.argv[1]
	out_file = sys.argv[2]
	main(dates_file, out_file)
