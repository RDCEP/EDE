import os, sys, subprocess
from netCDF4 import Dataset
from ede.models import Base, NetCDF_Meta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
    
def main(netcdf_filename):
    
    # Ingest meta data
    rootgrp = Dataset(netcdf_filename, "r", format="NETCDF4")
    
    dataset_name_ext = os.path.basename(netcdf_filename)
    dataset_name = os.path.splitext(dataset_name_ext)[0]
    
    dims = rootgrp.dimensions
    dims_names = []
    dims_sizes = []
    for dim_name, dim_info in dims.iteritems():
        dims_names.append(dim_name)
        dims_sizes.append(dim_info.size)
    
    variables = rootgrp.variables
    vars_names = []
    vars_dims = []
    vars_dims_nums = []
    vars_attrs = []
    vars_attrs_nums = []
    for var_name, var_info in variables.iteritems():
        vars_names.append(var_name)
        for dim in var_info.dimensions:
            vars_dims.append(dim)
        vars_dims_nums.append(var_info.ndim)
        attrs = var_info.ncattrs()
        for attr in attrs:
            vars_attrs.append(attr)
            vars_attrs.append(str(var_info.getncattr(attr)))
        vars_attrs_nums.append(len(attrs))
        
    rootgrp.close()

    engine = create_engine('postgresql://postgres:postgres@localhost:5432/ede')
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    new_netcdf_meta = NetCDF_Meta(dataset_name=dataset_name, dims_names=dims_names, dims_sizes=dims_sizes, vars_names=vars_names, \
        vars_dims=vars_dims, vars_dims_nums=vars_dims_nums, vars_attrs=vars_attrs, vars_attrs_nums=vars_attrs_nums)
    session.add(new_netcdf_meta)
    session.commit()
    
    # Ingest actual data through:
    # raster2pgsql -s 4326 -a -C -M -F -n "dataset_name" -t 10x10 papsim.nc4 netcdf_data | psql -d "ede" -U "postgres"
    p1 = subprocess.Popen(["raster2pgsql", "-s", "4326", "-a", "-C", "-M", "-F", "-n", "dataset_name", "-t", "10x10", netcdf_filename, "netcdf_data"],
        stdout=subprocess.PIPE)
    subprocess.Popen(["psql", "-d", "ede", "-U", "postgres"], stdin=p1.stdout)
    p1.stdout.close()

if __name__ == "__main__":
    netcdf_filename = sys.argv[1]
    main(netcdf_filename)