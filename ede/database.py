#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from ede.config import SQLALCHEMY_DATABASE_URI


engine = create_engine(
    SQLALCHEMY_DATABASE_URI, convert_unicode=True,
    pool_size=10)
db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine))


class EDEBase(object):
    id = Column(Integer, primary_key=True)

    @declared_attr
    def __tablename__(cls):
        return re.sub(r'(.)([A-Z])', r'\1_\2', cls.__name__).lower()


Base = declarative_base(cls=EDEBase)
Base.query = db_session.query_property()
db_session.session = db_session
