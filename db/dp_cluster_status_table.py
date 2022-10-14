#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Filename :dp_cluster_status_table.py
@Description :
@Datatime :2022/10/13 16:58:24
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
'''
from enum import unique
from operator import index
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.orm import relationship
from .database import Base


class ClusterStatus(Base):
    __tablename__ = 'dp_cluster_status_table'
    primary_id = Column(Integer, primary_key=True, index=True)  # 自增id
    cluster_name = Column(String, index=True, unique=True)  # 集群名称
    state = Column(String)  # 集群状态
    ip = Column(String(255), index=True, unique=True)  # 集群ip
    port = Column(Integer)  # 连接端口


class PartitionStatus(Base):
    __tablename__ = "dp_partition_table"
    primary_id = Column(Integer, primary_key=True)  # 自增id号
    cluster_name = Column(String(255), ForeignKey(
        ClusterStatus.cluster_name), index=True)  # slurm 名称
    partition_name = Column(String(255))  # patition 名称
    avail = Column(String(255))  # 是否可用
    nodes = Column(Integer)  # 总体节点数
    nodes_avial = Column(Integer)  # 可用节点数
    state = Column(String(255), index=True)  # 节点状态
    clusterstatus = relationship(
        "ClusterStatus", back_populates="dp_partition_table")
