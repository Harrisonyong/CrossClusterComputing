#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
@Filename :crud.py
@Description :
@Datatime :2022/10/14 14:22:46
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
"""

from sqlalchemy.orm import Session

from . import dp_cluster_status_table as models
from . import schema


def get_cluster_by_ip(db:Session, ip:str):
    """
    根据ip获取集群信息
    :param db: 数据库会话
    :param ip: 集群ip地址
    :return: 集群信息
    """
    return db.query(models.ClusterStatus).filter(models.ClusterStatus.ip == ip).first()


def get_cluster_by_name(db:Session, cluster_name:str):
    """
    根据集群名称获取集群信息
    :param db: 数据库会话
    :param cluster_name: 集群名称
    :return: 集群信息
    """
    return db.query(models.ClusterStatus).filter(models.ClusterStatus.cluster_name == cluster_name).first()

def get_clusters(db:Session, skip:int=0, limit:int=100):
    """
    获取特定数量的集群信息
    :param db: 数据库会话
    :param skip: 开始位置
    :param limit: 限制数量
    :return:集群信息列表
    """
    return db.query(models.ClusterStatus).offset(skip).limit(limit).all()

def create_cluster(db:Session, cluster:schema.ClusterCreate):
    """
    创建集群
    :param db:数据库会话
    :param cluster:集群模型
    :return:根据集群名称、ip、端口创建的集群信息
    """
    db_cluster = models.ClusterStatus(cluster_name=cluster.cluster_name, ip=cluster.ip, port = cluster.port, state = cluster.state, user=cluster.user, password = cluster.password)
    db.add(db_cluster)
    db.commit()
    db.refresh(db_cluster)
    return db_cluster


def get_partition_by_cluster_partition(db:Session, cluster_name: str, partition_name: str):
    """
    获取特定集群特定分区的节点信息
    :param db: 数据库会话
    :param cluster_name: 集群名称
    :param partition_name: 分区信息
    :return: 分区信息
    """
    return db.query(models.PartitionStatus).filter(models.PartitionStatus.cluster_name == cluster_name, 
    models.PartitionStatus.partition_name == partition_name).first()

def create_partition(db:Session, partition: schema.PartitionCreate):
    """
    创建集群分区
    :param db: 数据库会话
    :param partition: 分区模型
    :return: 根据集群名称、分区名称等创建的分区信息
    """
    db_partition = models.PartitionStatus(cluster_name = partition.cluster_name, partition_name = partition.partition_name, 
                                        avail = partition.avail, nodes = partition.nodes, nodes_avail = partition.nodes_avail, state =partition.state)
    db.add(db_partition)
    db.commit()
    db.refresh(db_partition)
    return db_partition

def get_partitions(db:Session, skip:int=0, limit:int=100):
    """
    获取特定数量的集群信息
    :param db: 数据库会话
    :param skip: 开始位置
    :param limit: 限制数量
    :return:集群信息列表
    """
    return db.query(models.PartitionStatus).offset(skip).limit(limit).all()

def update_partition(db:Session, cluster_name: str, partition_name:str, nodes:int, nodes_avail:int, avail:str, state:str):
    """
    更新集群分区信息
    :param db: 数据库会话
    :param cluster_name:集群名称
    :param partition_name:分区名称
    :param nodes:更新的节点数
    :param nodes_avail:可用节点数量
    :param avail:分区可用信息
    :param state:节点可用信息
    :return 更新的分区信息
    """
    db_partition = db.query(models.PartitionStatus).filter(models.PartitionStatus.cluster_name == cluster_name, models.PartitionStatus.partition_name == partition_name).update({"nodes":nodes, "nodes_avail":nodes_avail, "avail": avail, "state": state})
    db.commit()
    return db_partition