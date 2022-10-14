#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Filename :database.py
@Description :
@Datatime :2022/10/13 09:28:35
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
'''
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

#数据库连接
class Database:
    def __init__(self, db):
        self.engine=create_engine("sqlite:///"+db, echo=True, check_same_thread=False)
        self.session = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)

