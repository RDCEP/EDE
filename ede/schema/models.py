from ede.database import Base
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Raster
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
    rast = Column(Raster)