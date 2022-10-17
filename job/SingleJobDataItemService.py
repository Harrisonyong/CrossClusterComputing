#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/17 周一 14:38:21
# description: 单条作业数据处理服务,该文件中用于存储到数据表中

import sys
from pathlib import Path
from db.db_service import dbService
from db.dp_single_job_data_item_table import SingleJobDataItem

sys.path.append(str(Path(__file__).parent.parent))


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


print(sys.path)
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


if __name__ == '__main__':
    testAddBatch()
