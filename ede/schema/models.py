from ede.database import Base
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from geoalchemy2 import Raster, Geometry
from sqlalchemy.sql import func


class Grid_Meta(Base):
    __tablename__ = 'grid_meta'
    uid = Column(Integer, primary_key=True)
    filename = Column(String)
    filesize = Column(Integer)
    filetype = Column(String)
    meta_data = Column(JSONB)
    date_created = Column(DateTime)
    date_inserted = Column(DateTime, server_default=func.now())


class Grid_Var(Base):
    __tablename__ = 'grid_vars'
    uid = Column(Integer, primary_key=True)
    vname = Column(String, nullable=False, unique=True, index=True)


class Grid_Data_Lat_Lon(Base):
    __tablename__ = 'grid_data_lat_lon'
    uid = Column(Integer, primary_key=True)
    meta_id = Column(Integer, ForeignKey('grid_meta.uid'))
    var_id = Column(Integer, ForeignKey('grid_vars.uid'))
    rast = Column(Raster)


class Grid_Data_Time_Lat_Lon(Base):
    __tablename__ = 'grid_data_time_lat_lon'
    uid = Column(Integer, primary_key=True)
    meta_id = Column(Integer, ForeignKey('grid_meta.uid'))
    var_id = Column(Integer, ForeignKey('grid_vars.uid'))
    time_id = Column(Integer, ForeignKey('grid_times.uid'))
    rast = Column(Raster)


class Grid_Data_Depth_Lat_Lon(Base):
    __tablename__ = 'grid_data_depth_lat_lon'
    uid = Column(Integer, primary_key=True)
    meta_id = Column(Integer, ForeignKey('grid_meta.uid'))
    var_id = Column(Integer, ForeignKey('grid_vars.uid'))
    depth_id = Column(Integer, ForeignKey('grid_depths.uid'))
    rast = Column(Raster)


class Grid_Times(Base):
    __tablename__ = 'grid_times'
    uid = Column(Integer, primary_key=True)
    meta_id = Column(Integer, ForeignKey('grid_meta.uid'))
    time = Column(DateTime(timezone=True))


class Grid_Depths(Base):
    __tablename__ = 'grid_depths'
    uid = Column(Integer, primary_key=True)
    meta_id = Column(Integer, ForeignKey('grid_meta.uid'))
    depth = Column(Float)


class Regions_Meta(Base):
    __tablename__ = 'regions_meta'
    uid = Column(Integer, primary_key=True)
    name = Column(String)
    attributes = Column(ARRAY(String))


class Regions(Base):
    __tablename__ = 'regions'
    uid = Column(Integer, primary_key=True)
    meta_id = Column(Integer, ForeignKey('regions_meta.uid'))
    geom = Column(Geometry('GEOMETRY', srid=4326))
    meta_data = Column(JSONB)
