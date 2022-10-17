#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/14 Fri 15:27:45
# description: 该服务用于与数据库进行交互

from typing import List
from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import sessionmaker


from utils.config import Configuration
from sqlalchemy.ext.declarative import declarative_base

dbConfig = Configuration.dbConfig()
engine = create_engine(dbConfig["file"])
Session = sessionmaker(bind=engine)

Base = declarative_base()


class DBService:
    '数据库服务，统一进行数据库的增删改查'

    def query_all(self, Base) -> list:
        session = Session()
        items = session.query(Base).all()
        session.close()
        return items

    def addItem(self, item):
        session = Session()
        session.add(item)
        session.commit()
        session.close()
    
    def addBatchItem(self, items):
        session = Session()
        session.add_all(items)
        session.commit()
        session.close()

    def dbConfig():
        return dbConfig


dbService = DBService()
