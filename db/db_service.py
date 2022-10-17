#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/14 Fri 15:27:45
# description: 该服务用于与数据库进行交互

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utils.config import Configuration
from sqlalchemy.ext.declarative import declarative_base

__all__ = ["DBService", "engine", "Session", "Base"]

dbConfig = Configuration.dbConfig()
engine=create_engine(dbConfig["file"], connect_args={"check_same_thread": False})
Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)

Base = declarative_base()

class DBService:
    '数据库服务，统一进行数据库的增删改查'
    def query_all(self, Base):
        session = Session()
        items = session.query(Base).all()
        session.close()
        return items

    def addItem(self, Base):
        session = Session()
        session.add(Base)
        session.commit()
        session.close()
    

    def dbConfig():
        return dbConfig