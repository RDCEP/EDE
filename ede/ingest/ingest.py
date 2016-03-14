import os, sys, subprocess, time
from netCDF4 import Dataset
from osgeo import gdal
import psycopg2
from psycopg2.extras import Json
import re
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST

def main(netcdf_filename):
    
    ### Get meta data ###
    rootgrp = Dataset(netcdf_filename, "r", format="NETCDF4")
    
    # The dimensions
    dimensions = []
    for dim in rootgrp.dimensions.values():
        dimensions.append({
            "name":dim.name,
            "size":dim.size
        })
    
    # The variables        
    variables=[]
    for var in rootgrp.variables.values():
        # The dimensions the variable depends on
        dimensions=[]
        for dim in var.dimensions:
            dimensions.append(dim)
        # The attributes of the variable
        attributes=[]
        for attr in var.ncattrs():
            attributes.append({
                "name":attr,
                "value": str(var.getncattr(attr))
            })
        # The variable's info
        variables.append({
            "name":var.name,
            "dtype":str(var.dtype),
            "ndim":var.ndim,
            "shape":var.shape,
            "dimensions":dimensions,
            "attributes":attributes
        })
        print var
    
    # The global attributes
    attributes = []
    for attr_key in rootgrp.ncattrs():
        attributes.append({
            "name":attr_key,
            "value":rootgrp.getncattr(attr_key)
        })
     
    meta_data = {
        "name":os.path.basename(netcdf_filename),
        "dimensions":dimensions,
        "variables":variables,
        "attributes":attributes
    }

    # The bounding box
    geo = False
    
    if geo is False:
        try:
            lon1=float(min(rootgrp.variables['lon']))
            lon2=float(max(rootgrp.variables['lon']))
            lat1=float(min(rootgrp.variables['lat']))
            lat2=float(max(rootgrp.variables['lat']))
            geo=True
        except:
            pass

    if geo is False:
        try:
            lon1=float(min(rootgrp.variables['X']))
            lon2=float(max(rootgrp.variables['X']))
            lat1=float(min(rootgrp.variables['Y']))
            lat2=float(max(rootgrp.variables['Y']))
            geo=True
        except:
            pass

    if geo is False:
        try:
            lon1=float(min(rootgrp.variables['longitude']))
            lon2=float(max(rootgrp.variables['longitude']))
            lat1=float(min(rootgrp.variables['latitude']))
            lat2=float(max(rootgrp.variables['latitude']))
            geo=True
        except:
            pass

    if geo is True:
        meta_data["loc"]={
            "type": "Polygon",
            "coordinates": [[[lon1,lat1],[lon2,lat1],[lon2,lat2],[lon1,lat2],[lon1,lat1]]]
        }

    rootgrp.close()

    ## Connection to the database ##
    conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS,
                            host=DB_HOST, port=DB_PORT)
    cur = conn.cursor()
    
    # (1) Ingest into grid_meta + get meta_id
    cur.execute("insert into grid_meta (filename, filesize, filetype, meta_data, date_created) values (\'%s\', %s, \'%s\', %s, \'%s\') returning uid" %
        (os.path.basename(netcdf_filename), os.path.getsize(netcdf_filename), 'HDF', Json(meta_data), time.ctime(os.path.getctime(netcdf_filename))))
    rows = cur.fetchall()
    for row in rows:
        meta_id = int(row[0])
    
    # (2) Determine variables to loop over + loop over them
    vnames = []
    subdatasets = []
    gdal_dataset = gdal.Open(netcdf_filename)
    for sd in gdal_dataset.GetSubDatasets():
        vnames.append(sd[0].split(':')[-1])
    	subdatasets.append(sd[0])
    if not vnames:
        for var in rootgrp.variables.values():
            if var.name not in ['lat','lon','time']:
                vnames.append(var.name)

    p = re.compile('\\(\"rast\"\\)')
    q = re.compile('\\);')
    for i, vname in enumerate(vnames):
        # (3) Ingest into grid_vars + get var_id
        cur.execute("select uid from grid_vars where vname = \'%s\'" % (vname)) # check if variable already there
        rows = cur.fetchall()
        if not rows:
            cur.execute("insert into grid_vars (vname) values (\'%s\') returning uid" % (vname)) # insert if variable not already there
            rows = cur.fetchall()
        for row in rows:
            var_id = int(row[0])

        # (4) Ingest into grid_data
        # (4.1) Pipe the output of raster2pgsql into memory
        # The case where we don't have subdatasets, i.e. NetCDFs from Joshua
        if not subdatasets:
            # raster2pgsql -s 4326 -a -M -t 10x10 ../data/papsim.nc4 netcdf_data
            proc = subprocess.Popen(['raster2pgsql', '-s', '4326', '-a', '-t', '10x10', netcdf_filename, 'grid_data'], stdout=subprocess.PIPE)
        # The case where we do have subdatasets, i.e. NetCDFs from Alison
        else:
            # raster2pgsql -s 4326 -a -M -t 10x10 NETCDF:"../data/clim_0005_0043.tile.nc4":cropland netcdf_data
            proc = subprocess.Popen(['raster2pgsql', '-s', '4326', '-a', '-t', '10x10', subdatasets[i], 'grid_data'], stdout=subprocess.PIPE)
            
        # (4.2) Read output of raster2pgsql line by line, append (meta_id, var_id) + run the query into postgres
        while True:
            line = proc.stdout.readline().rstrip()
            if line == '':
                break
            elif line.startswith('INSERT INTO'):
                m = p.findall(line)
                subst_cols = p.subn('(\"rast\", \"meta_id\", \"var_id\")', line)[0]
                subst_all = q.subn(', %s, %s);' % (meta_id, var_id), subst_cols)[0]
                cur.execute(subst_all)

    conn.commit()
    
if __name__ == "__main__":
    netcdf_filename = sys.argv[1]
    main(netcdf_filename)
