#!/usr/bin/env python
# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from ede.config import SQLALCHEMY_DATABASE_URI


engine = create_engine(
    SQLALCHEMY_DATABASE_URI, convert_unicode=True,
    pool_size=10)
db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine))

Base = declarative_base()
Base.query = db_session.query_property()
db_session.session = db_session
