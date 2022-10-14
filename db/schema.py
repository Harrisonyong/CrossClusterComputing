#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Filename :schema.py
@Description :
@Datatime :2022/10/14 14:24:29
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
'''

from pydantic import BaseModel

from typing import List


class ClusterBase(BaseModel):
    cluster_name:str
    ip: str
    port:int

class Cluster(ClusterBase):
    primary_id:int
    state:str

class ClusterCreate(ClusterBase):
    state:str
    

class PartitionBase(BaseModel):
    pass
