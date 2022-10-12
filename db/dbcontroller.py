#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Description : 该文件用于接收REST请求，并测试程序
@Datatime :2022-10-11 15:42:43
@Author :songquanheng
@email :wannachan@outlook.com
'''
import sys
from datetime import datetime
from pathlib import Path
from urllib import response
from fastapi import APIRouter

sys.path.append(str(Path(__file__).parent.parent))
from utils.config import Configuration as config

from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect
from .dp_cluster_status_table import ClusterStatus


router = APIRouter(
    prefix="/db-controller",
    tags=["db-controller"],
    responses={404: {"description": "db error"}}
)

@router.get("/welcome")
async def welcome():
   return {"message": "Welcom To db-controller"}


@router.get("/db-config")
async def dbConfig():
    dbConfig = config.dbConfig()
    host = dbConfig["host"]
    print("host="+ host)
    file= dbConfig["file"]
    print("file="+ file)
    
    return dbConfig

@router.get("/all-clusters")
async def allClusters():
    dbConfig = config.dbConfig()
    file= dbConfig["file"]
    print("file="+ file)
    engine=create_engine("sqlite:///"+file)
    Session = sessionmaker(bind=engine)
    session = Session()
    clusters = session.query(ClusterStatus).all()

    return clusters

@router.get("/add-cluster")
async def addCluster():
    dbConfig = config.dbConfig()
    file= dbConfig["file"]
    print("file="+ file)
    engine=create_engine("sqlite:///"+file)
    Session = sessionmaker(bind=engine)
    session = Session()
    cluster=ClusterStatus(
        name = "slurm1",
        state = "online",
        ip = "0.0.0.0",
        user_name = "root",
        password = "root"
        )
    session.add(cluster)
    session.commit()
    return "插入数据成功: "