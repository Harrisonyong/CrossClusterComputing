#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/17 周一 14:38:21
# description: 单条作业数据处理服务,该文件中用于存储到数据表中

from sqlalchemy.ext.declarative import declarative_base
from utils.config import Configuration
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, func
from db.dp_single_job_data_item_table import SingleJobDataItem
from db.db_service import dbService
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

    def query_all(self):
        '查询作业数据表中的全部记录'
        return dbService.query_all(SingleJobDataItem)

    def groupByJobTotalId(self):
        """返回分组，0号位为job_total_id, 1号位为待处理的数据文件数量"""
        session = Session()
        result = session.query(SingleJobDataItem.job_total_id, func.count(
            SingleJobDataItem.primary_id)).group_by(SingleJobDataItem.job_total_id).all()
        session.close()
        return result

    def allJobTotalId(self) -> List[int]:
        '''使用列表推导式计算出所有的整体作业号'''
        session = Session()
        result = session.query(SingleJobDataItem.job_total_id).distinct(
            SingleJobDataItem.job_total_id).all()
        session.close
        return [item[0] for item in result]


singleJobDataItemService = SingleJobDataItemService()


def testAddBatch():
    dataItems = []
    for i in range(10):
        item = SingleJobDataItem(
            job_total_id=1,
            data_file="/root/msa/"+str(i)+".ciff",
        )
        dataItems.append(item)

    dbService.addBatchItem(dataItems)


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
    testGroup()
