#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Filename :dp_cluster_status_table.py
@Description :
@Datatime :2022/10/13 16:58:24
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
'''
from ast import Str
from datetime import datetime
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, func
from sqlalchemy.orm import relationship

from db.dp_job_data_submit_table import JobDataSubmit
from .db_service import Base


class ClusterStatus(Base):
    __tablename__ = 'dp_cluster_status_table'
    primary_id = Column(Integer, primary_key=True, index=True)  # 自增id
    cluster_name = Column(String, index=True, unique=True)  # 集群名称
    state = Column(String)  # 集群状态
    ip = Column(String(255), index=True)  # 集群ip
    port = Column(Integer)  # 连接用户
    user = Column(String(255)) # 连接密码
    password = Column(String(255))
    createtime = Column(DateTime(timezone=True), default = datetime.now, comment = "创建时间")
    updatetime = Column(DateTime(timezone=True), default = datetime.now, onupdate = datetime.now, comment = "修改时间")
    partitions = relationship("PartitionStatus", back_populates = "clusterstatus")
    def __repr__(self) -> str:
        return "<ClusterStatus(clustername=%s, ip=%s, state=%s)>" %(self.cluster_name, self.ip, self.state)
    


class PartitionStatus(Base):
    __tablename__ = "dp_partition_table"
    primary_id = Column(Integer, primary_key=True)  # 自增id号
    cluster_name = Column(String(255), ForeignKey(
        ClusterStatus.cluster_name), index=True)  # slurm 名称
    partition_name = Column(String(255))  # patition 名称
    avail = Column(String(255))  # 是否可用
    nodes = Column(Integer)  # 总体节点数
    nodes_avail = Column(Integer)  # 可用节点数
    state = Column(String(255), index=True)  # 节点状态
    createtime = Column(DateTime(timezone=True), default = datetime.now, comment = "创建时间")
    updatetime = Column(DateTime(timezone=True), default = datetime.now, onupdate = datetime.now, comment = "修改时间")
    clusterstatus = relationship(
        "ClusterStatus", back_populates="partitions")

    def __repr__(self) -> str:
        return "<PartitionStatus(clustername=%s, partion_name=%s, nodes_avail=%s, clusterstatus=%s)>" %(self.cluster_name, self.partition_name, self.nodes_avail, self.clusterstatus)

    def canSchdule(self, record: JobDataSubmit) -> bool:
        "判断该分区是否可以调度作业投递记录"
        return self.nodes_avail /(1.0*record.nodesNeeded()) >= 1
        
    def numberCanSchdule(self, record: JobDataSubmit) -> int:
        """
        该分区能够调度的作业数目
        若作业概览记录需要2个节点完成一个条目，而该分区为1，则可调度的作业条目数为0
        若分区的节点数为3，则可以调度1个作业条目
        若分区的节点数为4，则可以调度的作业条目数为2
        """
        
        return int(self.nodes_avail /(1.0*record.nodesNeeded()))