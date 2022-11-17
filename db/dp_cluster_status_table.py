#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Filename :dp_cluster_status_table.py
@Description :
@Datatime :2022/10/13 16:58:24
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
'''
from datetime import datetime
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from db.dp_job_data_submit_table import JobDataSubmit
from db.db_service import Base


class ClusterStatus(Base):
    __tablename__ = 'dp_cluster_status_table'
    primary_id = Column(Integer, primary_key=True, index=True)  # 自增id
    cluster_name = Column(String, index=True, unique=True)  # 集群名称
    state = Column(String)  # 集群状态
    ip = Column(String(255), index=True)  # 集群ip
    port = Column(Integer)  # 连接用户
    user = Column(String(255))  # 连接密码
    password = Column(String(255))
    # 集群中最大运行作业限制，该信息会限制CCC投递作业，
    # 当集群中已经运行的作业数目大于等于该值，不允许向该集群递交新的作业
    max_submit_jobs_limit = Column(Integer)
    submit_jobs_num = Column(Integer)
    createtime = Column(DateTime(timezone=True), default=datetime.now, comment="创建时间")
    updatetime = Column(DateTime(timezone=True), default=datetime.now, onupdate=datetime.now, comment="修改时间")
    partitions = relationship("PartitionStatus", back_populates="clusterstatus")

    def __repr__(self) -> str:
        return "<ClusterStatus(clustername=%s, ip=%s, state=%s)>" % (self.cluster_name, self.ip, self.state)

    def can_submit_new_job(self):
        """
        判断是否可以向集群提交新的作业，当且仅当作业的提交数小于限制时才可以提交
        @return:
        """
        return self.submit_jobs_num < self.max_submit_jobs_limit


class PartitionStatus(Base):
    __tablename__ = "dp_partition_table"
    primary_id = Column(Integer, primary_key=True)  # 自增id号
    cluster_name = Column(String(255), ForeignKey(
        ClusterStatus.cluster_name), index=True)  # slurm 名称
    partition_name = Column(String(255))  # partition 名称
    avail = Column(String(255))  # 是否可用
    nodes = Column(Integer)  # 总体节点数
    nodes_avail = Column(Integer)  # 可用节点数
    state = Column(String(255), index=True)  # 节点状态
    createtime = Column(DateTime(timezone=True), default=datetime.now, comment="创建时间")
    updatetime = Column(DateTime(timezone=True), default=datetime.now, onupdate=datetime.now, comment="修改时间")
    clusterstatus = relationship(
        "ClusterStatus", back_populates="partitions")

    def __repr__(self) -> str:
        return f"<PartitionStatus(cluster_name={self.cluster_name}, partition_name={self.partition_name}, nodes_avail={self.nodes_avail}, clusterstatus={self.clusterstatus})>"

    def can_schedule(self, record: JobDataSubmit) -> bool:
        """判断该分区是否可以调度作业投递记录"""
        return self.nodes_avail / (1.0 * record.nodes_needed()) >= 1

    def number_can_schedule(self, record: JobDataSubmit) -> int:
        """
        该分区能够调度的作业数目
        若作业概览记录需要2个节点完成一个条目，而该分区为1，则可调度的作业条目数为0
        若分区的节点数为3，则可以调度1个作业条目
        若分区的节点数为4，则可以调度的作业条目数为2
        """

        return int(self.nodes_avail / (1.0 * record.nodes_needed()))
