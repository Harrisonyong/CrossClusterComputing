#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
@Description : 该文件用于接收REST请求，并测试程序
@Datetime :2022-10-11 15:42:43
@Author :songquanheng
@email :wannachan@outlook.com
"""
import sys
from pathlib import Path
from fastapi import APIRouter
from utils.response import Response

sys.path.append(str(Path(__file__).parent.parent))
from db.dp_cluster_status_table import ClusterStatus
from db.db_service import dbService

router = APIRouter(
    prefix="/db-controller",
    tags=["db-controller"],
    responses={404: {"description": "db error"}}
)


@router.get("/welcome")
async def welcome():
    return {"message": "Welcome To db-controller"}


@router.get("/db-config")
async def db_config():
    return Response.success(msg="查询集群配置成功", data=dbService.db_config())


@router.get("/all-clusters")
async def all_clusters():
    clusters = dbService.query_all(ClusterStatus)
    return Response.success(msg="查询集群所有数据成功", data=clusters)


@router.get("/add-cluster")
async def add_cluster():
    cluster = ClusterStatus(
        name="slurm1",
        state="online",
        ip="0.0.0.0",
        user_name="root",
        password="root"
    )
    dbService.add_item(cluster)
    return Response.success(msg="数据插入成功", data=cluster)
