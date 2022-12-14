#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Filename :schema.py
@Description :
@Datatime :2022/10/14 14:24:29
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
'''

from datetime import datetime
from typing import List

from pydantic import BaseModel


class PartitionBase(BaseModel):
    avail: str
    nodes: int
    nodes_avail: int
    state: str


class Partition(PartitionBase):
    """分区映射表"""
    primary_id: int
    cluster_name: str
    partition_name: str
    createtime: datetime
    updatetime: datetime

    class Config:
        orm_mode = True


class PartitionCreate(PartitionBase):
    cluster_name: str
    partition_name: str


class ParitionUpdate(PartitionBase):
    pass


class ClusterBase(BaseModel):
    cluster_name:str
    ip: str
    port:int
    user: str


class Cluster(ClusterBase):
    primary_id: int
    state: str
    createtime: datetime
    updatetime: datetime
    partitions: List[Partition] = []

    class Config:
        orm_mode = True


class ClusterCreate(ClusterBase):
    state: str
    password: str
    max_submit_jobs_limit: int
