from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Raster
from sqlalchemy.sql import func

Base = declarative_base()

class Grid_Meta(Base):
    __tablename__ = 'grid_meta'
    filename = Column(String)
    filesize = Column(Integer)
    filetype = Column(String)
    meta_data = Column(JSONB)
    date_created = Column(DateTime)
    date_inserted = Column(DateTime, server_default=func.now())
 
class Grid_Var(Base):
    __tablename__ = 'grid_vars'
    vname = Column(String, nullable=False, unique=True, index=True)
   
class Grid_Data(Base):
    __tablename__ = 'grid_data'
    meta_id = Column(Integer, ForeignKey('grid_meta.uid'))
    var_id = Column(Integer, ForeignKey('grid_vars.uid'))
    rast = Column(Raster)