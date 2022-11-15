#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/20 周四 17:26:37
# description: 封装与数据库中集群的使用


import sys
from pathlib import Path
from typing import List
sys.path.append(str(Path(__file__).parent.parent))
from db.dp_cluster_status_table import ClusterStatus
from db.db_service import Session


class DBClusterService:

    def get_cluster_by_ip(self, ip: str) -> ClusterStatus:
        """
        根据ip获取集群信息
        :param db: 数据库会话
        :param ip: 集群ip地址
        :return: 集群信息
        """
        with Session() as session:
            return session.query(ClusterStatus).filter(ClusterStatus.ip == ip).first()

    def get_cluster_by_name(self, cluster_name: str) -> ClusterStatus:
        """
        根据集群名称获取集群信息
        :param db: 数据库会话
        :param cluster_name: 集群名称
        :return: 集群信息
        """
        with Session() as session:
            return session.query(ClusterStatus).filter(ClusterStatus.cluster_name == cluster_name).first()

    def get_clusters(self, skip: int = 0, limit: int = 100)-> List[ClusterStatus]:
        """
        获取特定数量的集群信息
        :param db: 数据库会话
        :param skip: 开始位置
        :param limit: 限制数量
        :return:集群信息列表
        """
        with Session() as session:
            return session.query(ClusterStatus).offset(skip).limit(limit).all()


dBClusterService = DBClusterService()
