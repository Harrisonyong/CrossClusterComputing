#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/20 周四 17:25:14
# description: 封装对于分区的查询

import sys
from pathlib import Path
from typing import List

sys.path.append(str(Path(__file__).parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, selectinload
from db.dp_cluster_status_table import PartitionStatus
from sqlalchemy.ext.declarative import declarative_base
from utils.config import dbConfig

__all__ = ["engine", "Session", "Base"]

engine = create_engine(dbConfig["file"], connect_args={"check_same_thread": False})
Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


class DBPartionService:
    """该接口用于封装于数据库分区的使用"""

    def get_partition_by_cluster_partition(self, cluster_name: str, partition_name: str) -> PartitionStatus:
        """
        获取特定集群特定分区的节点信息
        :param db: 数据库会话
        :param cluster_name: 集群名称
        :param partition_name: 分区信息
        :return: 分区信息
        """
        with Session() as session:
            return session.query(PartitionStatus) \
                .options(selectinload(PartitionStatus.clusterstatus)) \
                .filter(PartitionStatus.cluster_name == cluster_name, PartitionStatus.partition_name == partition_name) \
                .first()

    def get_partitions(self, skip: int = 0, limit: int = 100) -> List[PartitionStatus]:
        """
        获取特定数量的分区信息
        :param db: 数据库会话
        :param skip: 开始位置
        :param limit: 限制数量
        :return:集群信息列表
        """
        with Session() as session:
            return session.query(PartitionStatus) \
                .options(selectinload(PartitionStatus.clusterstatus)) \
                .offset(skip).limit(limit).all()

    def get_available_partitions(self, skip: int = 0, limit: int = 1000) -> List[PartitionStatus]:
        """
        获取可用的的分区信息, 此时会直接调用获取分区的集群信息, 并基于可用节点进行排序
        :param db: 数据库会话
        :param skip: 开始位置
        :param limit: 限制数量
        :return:分区信息列表
        """
        with Session() as session:
            return session.query(PartitionStatus) \
                .options(selectinload(PartitionStatus.clusterstatus)) \
                .filter(PartitionStatus.nodes_avail > 0).offset(skip).limit(limit).all()


dBPartitionService = DBPartionService()


def test():
    print(dBPartitionService.get_available_partitions())
    print(dBPartitionService.get_partition_by_cluster_partition("slurm1", "allNodes").clusterstatus)
    print(dBPartitionService.get_partitions())


if __name__ == '__main__':
    test()
