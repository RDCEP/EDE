from ede.database import Base
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
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
   
class Grid_Data(Base):
    __tablename__ = 'grid_data'
    uid = Column(Integer, primary_key=True)
    meta_id = Column(Integer, ForeignKey('grid_meta.uid'))
    var_id = Column(Integer, ForeignKey('grid_vars.uid'))
    date = Column(Integer, ForeignKey('grid_times.uid'))
    rast = Column(Raster)

class Grid_Times(Base):
    __tablename__ = 'grid_times'
    uid = Column(Integer, primary_key=True)
    meta_id = Column(Integer, ForeignKey('grid_meta.uid'))
    date = Column(DateTime(timezone=True))

class Regions_Meta(Base):
    __tablename__ = 'regions_meta'
    uid = Column(Integer, primary_key=True)
    name = Column(String)
    version = Column(String)
    attributes = Column(ARRAY(String))

class Regions(Base):
    __tablename__ = 'regions'
    uid = Column(Integer, primary_key=True)
    meta_id = Column(Integer, ForeignKey('regions_meta.uid'))
    geom = Column(Geometry('POLYGON'))
    meta_data = Column(JSONB)