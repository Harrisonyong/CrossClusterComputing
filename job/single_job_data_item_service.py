#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/17 周一 14:38:21
# description: 单条作业数据处理服务,该文件中用于存储到数据表中

import sys
from pathlib import Path
from typing import List
sys.path.append(str(Path(__file__).parent.parent))

from db.db_service import dbService
from db.dp_single_job_data_item_table import SingleJobDataItem
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine(dbService.db_config()["file"], connect_args={
                       "check_same_thread": False})
Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


class SingleJobDataItemService:
    @staticmethod
    def add(job_total_id, data_file):
        """新增一条作业数据条目服务到数据表中"""
        data_item = SingleJobDataItem()
        data_item.job_total_id = job_total_id
        data_item.data_file = data_file
        dbService.add_item(data_item)

    @staticmethod
    def add_batch(single_job_data_items: List[SingleJobDataItem]):
        """添加一组作业数据条目到数据库中"""
        dbService.add_batch_item(single_job_data_items)

    @staticmethod
    def query_all():
        """查询作业数据表中的全部记录"""
        return dbService.query_all(SingleJobDataItem)

    @staticmethod
    def group_by_job_total_id():
        """返回分组，0号位为job_total_id, 1号位为待处理的数据文件数量"""
        with Session() as session:
            return session.query(SingleJobDataItem.job_total_id, func.count(
                SingleJobDataItem.primary_id)).group_by(SingleJobDataItem.job_total_id).all()

    @staticmethod
    def all_job_total_id() -> List[int]:
        """使用列表推导式计算出所有的整体作业号"""
        with Session() as session:
            result = session.query(SingleJobDataItem.job_total_id).distinct(
                SingleJobDataItem.job_total_id).all()
            return [item[0] for item in result]

    @staticmethod
    def count_of_items(job_total_id: int):
        """指定作业号的条数"""
        return 100

    @staticmethod
    def query_according_id_and_limit(job_total_id: int, limit: int) -> List[SingleJobDataItem]:
        """查询系统中属于job_total_id的指定数量的作业数据条目"""
        with Session() as session:
            return session.query(SingleJobDataItem).filter(SingleJobDataItem.job_total_id == job_total_id).limit(limit).all()

    @staticmethod
    def delete_batch(ids: List[int]) -> int:
        """
        批量删除单条作业条目
        返回删除的行数
        """
        with Session() as session:
            session.query(SingleJobDataItem).filter(SingleJobDataItem.primary_id.in_(ids)).delete(synchronize_session=False)
            session.commit()
        return


singleJobDataItemService = SingleJobDataItemService()


def test_add_batch():
    data_items = []
    for i in range(10):
        item = SingleJobDataItem(
            job_total_id=1,
            data_file="/root/msa/"+str(i)+".ciff",
        )
        data_items.append(item)

    SingleJobDataItemService.add_batch(data_items)


def test_group():
    groups = SingleJobDataItemService.group_by_job_total_id()
    print(type(groups))
    print(type(groups[0]))
    for group in groups:
        print(group[0], group[1])


def test_distinct():
    job_total_ids = singleJobDataItemService.all_job_total_id()
    print(job_total_ids)
    print(type(job_total_ids))
    print(type(job_total_ids[0]))
    print(job_total_ids)


def test_delete():

    SingleJobDataItemService.delete_batch([64, 65, 66, 67, 68])


if __name__ == '__main__':
    test_delete()
