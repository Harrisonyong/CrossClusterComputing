#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Filename :schema.py
@Description :
@Datatime :2022/10/14 14:24:29
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
'''

from db.dp_cluster_status_table import PartitionStatus
from pydantic import BaseModel

from typing import List
from datetime import datetime

class PartitionBase(BaseModel):
    avail: str
    nodes: int
    nodes_avail: int
    state: str
    partition_name: str

class Partition(PartitionBase):
    primary_id: int
    cluster_name: str
    createtime: datetime
    updatetime: datetime
    class Config:
        orm_mode = True

class PartitionCreate(PartitionBase):
    cluster_name: str

class ClusterBase(BaseModel):
    cluster_name:str
    ip: str
    port:int

class Cluster(ClusterBase):
    primary_id:int
    state:str
    createtime: datetime
    updatetime: datetime
    partitions: List[Partition] = []

    class Config:
        orm_mode = True

class ClusterCreate(ClusterBase):
    state:str
