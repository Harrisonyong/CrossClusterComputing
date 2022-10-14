#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Filename :crud.py
@Description :
@Datatime :2022/10/14 14:22:46
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
'''

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
    db_cluster = models.ClusterStatus(cluster_name=cluster.cluster_name, ip=cluster.ip, port = cluster.port, state = cluster.state)
    db.add(db_cluster)
    db.commit()
    db.refresh(db_cluster)
    return db_cluster