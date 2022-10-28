#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/17 周一 14:38:21
# description: 单条作业数据处理服务,该文件中用于存储到数据表中

from db.db_service import dbService
from db.dp_single_job_data_item_table import SingleJobDataItem
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from utils.config import Configuration
from sqlalchemy.ext.declarative import declarative_base
import sys
from pathlib import Path
from typing import List
sys.path.append(str(Path(__file__).parent.parent))


engine = create_engine(dbService.dbConfig()["file"], connect_args={
                       "check_same_thread": False})
Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


class SingleJobDataItemService:
    def add(self, job_total_id, data_file):
        '新增一条作业数据条目服务到数据表中'
        dataItem = SingleJobDataItem()
        dataItem.job_total_id = job_total_id
        dataItem.data_file = data_file
        dbService.addItem(dataItem)

    def addBatch(self, singleJobDataItems: List[SingleJobDataItem]):
        '添加一组作业数据条目到数据库中'
        assert len(singleJobDataItems) > 0, "确保存在插入数据库的作业数据条目"
        print("待处理的作业号为: ", singleJobDataItems[0].job_total_id)
        dbService.addBatchItem(singleJobDataItems)

    def query_all(self):
        '查询作业数据表中的全部记录'
        return dbService.query_all(SingleJobDataItem)

    def groupByJobTotalId(self):
        """返回分组，0号位为job_total_id, 1号位为待处理的数据文件数量"""
        with Session() as session:
            return session.query(SingleJobDataItem.job_total_id, func.count(
                SingleJobDataItem.primary_id)).group_by(SingleJobDataItem.job_total_id).all()

    def allJobTotalId(self) -> List[int]:
        '''使用列表推导式计算出所有的整体作业号'''
        with Session() as session:
            result = session.query(SingleJobDataItem.job_total_id).distinct(
                SingleJobDataItem.job_total_id).all()
            return [item[0] for item in result]

    def countOfItems(self, job_total_id: int):
        """指定作业号的条数"""
        return 100

    def queryAccordingIdAndLimit(self, job_total_id: int, limit: int) -> List[SingleJobDataItem]:
        """查询系统中属于job_total_id的指定数量的作业数据条目"""
        with Session() as session:
            return session.query(SingleJobDataItem).limit(limit).all()


singleJobDataItemService = SingleJobDataItemService()


def testAddBatch():
    dataItems = []
    for i in range(10):
        item = SingleJobDataItem(
            job_total_id=1,
            data_file="/root/msa/"+str(i)+".ciff",
        )
        dataItems.append(item)

    singleJobDataItemService.addBatch(dataItems)


def testGroup():
    groups = singleJobDataItemService.groupByJobTotalId()
    print(type(groups))
    print(type(groups[0]))
    for group in groups:
        print(group[0], group[1])


def testDistinct():
    jobTotalIds = singleJobDataItemService.allJobTotalId()
    print(jobTotalIds)
    print(type(jobTotalIds))
    print(type(jobTotalIds[0]))
    print(jobTotalIds)


if __name__ == '__main__':
    lang = ["Python", "C++", "Java", "PHP", "Ruby", "MATLAB"]
    for i in range(len(lang) - 1):
        lang.pop(i)
    print(lang)
