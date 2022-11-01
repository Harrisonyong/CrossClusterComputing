#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Filename :monitor.py
@Description :
@Datatime :2022/09/30 16:37:43
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
'''

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from collections import defaultdict
from typing import List
from utils.log import Log
from utils.scheduler import Scheduler
from utils.config import Configuration as config
from utils.response import Response
from fastapi import APIRouter, Depends, HTTPException
from slurm_monitor.serverconn import SlurmServer
from sqlalchemy.orm import Session
from db import dp_cluster_status_table as models
from db import db_service as database
from db import crud, schema

models.Base.metadata.create_all(bind=database.engine)

log = Log.ulog("monitor.log")
router = APIRouter(
    prefix="/monitor",
    tags=["monitor"],
    responses={404: {"description": "Not Found any slurm server"}}
)
scheduler = Scheduler.AsyncScheduler()
def get_db():
    db = database.Session()
    try:
        yield db
    finally:
        db.close()

def partition_parse(std_out):
    """解析slurm输出的分区信息
    param: std_out :集群分区信息的查询结果
    result: partition_dict:集群中分区和节点对应关系
    """
    partition_dict = defaultdict(dict)
    next(std_out)
    for line in std_out:
        info = line.strip("\n").split()
        partition_name, avail, nodes, state = info[0], info[1], info[3], info[4]
        partition_dict[partition_name].setdefault("state", "alloc")
        partition_dict[partition_name].setdefault("avail_nodes", 0)
        partition_dict[partition_name].setdefault("nodes", 0)
        partition_dict[partition_name]["avail"] = avail
        partition_dict[partition_name]["nodes"] += int(nodes)
        if state == "idle":
            partition_dict[partition_name]["state"] = state
            partition_dict[partition_name]["avail_nodes"] += int(nodes)
    return partition_dict

def slurm_search(name, host, port, user, password):
    with SlurmServer(host=host, port=port, user=user, password=password) as slurm:
        std_out, std_err = slurm.sinfo()
        partition_dict = partition_parse(std_out=std_out)
        if std_err: log.error(std_err.read().decode("utf8"))

    with database.Session() as db:
        for partition_name, info in partition_dict.items():
            partition = schema.PartitionCreate(cluster_name = name, partition_name = partition_name, nodes = info.get("nodes"), nodes_avail = info.get("avail_nodes"), avail = info.get("avail"), state=info.get("state"))
            log.info(create_partition(partition=partition, db=db))

def add_slurm_monitor_job(seconds):
    with database.Session() as db:
        for name, conf in config.ServiceConfig():
            host, port, user, password = conf.host, conf.port, conf.user, conf.password
            cluster = schema.ClusterCreate(cluster_name=name, ip=host, port=port, user=user, password=password,state="avail")
            db_cluster = crud.get_cluster_by_name(db, cluster_name=name)
            if db_cluster:
                print(f'db-{name} is exists')
            else:
                crud.create_cluster(db, cluster=cluster)
            print(f"host = {host}, port={port}, user={user}, password={password}")
            scheduler.add_job(slurm_search, args=[
                            name,host, port, user, password], id=f"{name}", trigger="interval", seconds=seconds, replace_existing=True)
            print(f"定时监控任务{name}启动")

@router.post("/cluster/", response_model=schema.Cluster)
def create_cluster(cluster:schema.ClusterCreate, db:Session = Depends(get_db)):
    db_cluster = crud.get_cluster_by_name(db,  cluster_name = cluster.cluster_name)
    if db_cluster:
        raise HTTPException(status_code=400, detail = "cluster is already existed")
    return crud.create_cluster(db=db, cluster=cluster)

@router.get("/clusters/", response_model=List[schema.Cluster])
def read_clusters(skip:int=0, limit:int=100, db:Session=Depends(get_db)):
    clusters = crud.get_clusters(db, skip=skip, limit=limit)
    return clusters

@router.get("/clust/{ip}", response_model=schema.Cluster)
def read_cluster(ip:str, db:Session=Depends(get_db)):
    db_cluster = crud.get_cluster_by_ip(db, ip = ip)
    if db_cluster is None:
        raise HTTPException(status_code=404, detail="cluster not found")
    return db_cluster

@router.post("/partition/")
def create_partition(partition:schema.PartitionCreate, db:Session = Depends(get_db)):
    result = crud.update_partition(db=db, cluster_name=partition.cluster_name, partition_name=partition.partition_name, nodes=partition.nodes, nodes_avail=partition.nodes_avail, avail=partition.avail, state=partition.state)
    if result:
        return Response.success(msg="update success", data=dict(partition))
    else:
        crud.create_partition(db=db, partition=partition)
        return Response.success(msg="create success")

@router.get("/partitions/", response_model=List[schema.Partition])
def get_partitions(skip:int=0, limit:int=100, db:Session=Depends(get_db)):
    partitions = crud.get_partitions(db, skip=skip, limit=limit)
    return partitions

@router.put("/part/{cluster_name}/{partition_name}/", response_model=schema.Partition)
def relate_cluster_partition(cluster_name: str, partition_name: str, db:Session = Depends(get_db)):
    partition = crud.get_partition_by_cluster_partition(db=db, cluster_name=cluster_name, partition_name=partition_name)
    return partition
