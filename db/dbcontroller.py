#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Description : 该文件用于接收REST请求，并测试程序
@Datatime :2022-10-11 15:42:43
@Author :songquanheng
@email :wannachan@outlook.com
'''
import sys
from pathlib import Path
from fastapi import APIRouter

from utils.response import Response

sys.path.append(str(Path(__file__).parent.parent))
from utils.config import Configuration as config

from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import sessionmaker
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
    return config.dbConfig()

@router.get("/all-clusters")
async def allClusters():
    dbConfig = config.dbConfig()
    engine=create_engine(dbConfig["file"])
    Session = sessionmaker(bind=engine)
    session = Session()
    clusters = session.query(ClusterStatus).all()

    return Response.success(msg="查询集群所有数据成功", data=clusters)

@router.get("/add-cluster")
async def addCluster():
    dbConfig = config.dbConfig()
    engine=create_engine(dbConfig["file"])
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
    return Response.success(msg="数据插入成功", data=cluster)