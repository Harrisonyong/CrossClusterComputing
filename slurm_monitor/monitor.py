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
from typing import List
from unicodedata import name
sys.path.append(str(Path(__file__).parent.parent))
from utils.log import Log
from utils.scheduler import Scheduler
from utils.config import Configuration as config
from fastapi import APIRouter, Depends, HTTPException
from slurm_monitor.serverconn import SlurmServer
from sqlalchemy.orm import Session
from db import dp_cluster_status_table as models
from db import db_service as database
from db import crud, schema

models.Base.metadata.create_all(bind=database.engine)

router = APIRouter(
    prefix="/monitor",
    tags=["monitor"],
    responses={404: {"description": "Not Found any slurm server"}}
)
logger = Log.ulog("monitor.log")
scheduler = Scheduler.AsyncScheduler()

def get_db():
    db = database.Session()
    try:
        yield db
    finally:
        db.close()

def slurm_search(name, host, port, user, password):
    command = "export PATH=/usr/local/slurm-21.08.8/bin; sinfo"
    slurm = SlurmServer(host=host, port=port, user=user, password=password)
    std_out, std_err = slurm.exec(command=command)
    for line in std_out:
        if "idle" and "up" in line:
            info = line.strip("\n").split()
            avail_partition, avail_nodes = info[0], info[3]
            print(
                f"{host}: avail_partition:{avail_partition}-avail_nodes:{avail_nodes}")
    print(std_err.read().decode("utf8"))
    slurm.close()

def add_slurm_monitor_job(seconds):
    with database.Session() as db:
        for name, conf in config.ServiceConfig():
            host, port, user, password = conf.host, conf.port, conf.user, conf.password
            cluster = schema.ClusterCreate(cluster_name=name, ip=host, port=port, user=user, password=password,state="avail")
            db_cluster = crud.get_cluster_by_name(db, cluster_name=name)
            if db_cluster:
                print(f'{name} is exists')
            else:
                crud.create_cluster(db, cluster=cluster)
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
    if result: return {202: "update success"}
    else:
        crud.create_partition(db=db, partition=partition)
        return {201: "create success"}


@router.get("/partitions/", response_model=List[schema.Partition])
def get_partitions(skip:int=0, limit:int=100, db:Session=Depends(get_db)):
    partitions = crud.get_partitions(db, skip=skip, limit=limit)
    return partitions


@router.put("/part/{cluster_name}/{partition_name}/", response_model=schema.Partition)
def relate_cluster_partition(cluster_name: str, partition_name: str, db:Session = Depends(get_db)):
    partition = crud.get_partition_by_cluster_partition(db=db, cluster_name=cluster_name, partition_name=partition_name)
    return partition