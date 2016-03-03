from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Raster
from sqlalchemy.sql import func

Base = declarative_base()

class Global_Meta(Base):
    __tablename__ = 'global_meta'
    gid = Column(Integer, primary_key=True)
    filename = Column(String)
    filesize = Column(Integer)
    date_created = Column(DateTime)
    date_inserted = Column(DateTime, server_default=func.now())

class NetCDF_Meta(Base):
    __tablename__ = 'netcdf_meta'
    mid = Column(Integer, primary_key=True)
    gid = Column(Integer, ForeignKey('global_meta.gid'))
    meta_data = Column(JSONB)
 
class NetCDF_Var(Base):
    __tablename__ = 'netcdf_vars'
    vid = Column(Integer, primary_key=True)
    vname = Column(String, nullable=False, unique=True, index=True)
   
class NetCDF_Data(Base):
    __tablename__ = 'netcdf_data'
    rid = Column(Integer, primary_key=True)
    gid = Column(Integer, ForeignKey('global_meta.gid'))
    vid = Column(Integer, ForeignKey('netcdf_vars.vid'))
    rast = Column(Raster)