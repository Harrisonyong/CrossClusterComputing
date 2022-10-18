#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/13 周四 17:15:17
# description: 该文件负责把用户的作业投递请求创建修改成作业投递记录，存入作业数据投递数据表中


import time
import os
from typing import List
from db.db_service import DBService
from db.dp_job_data_submit_table import JobDataSubmit
from db.dp_single_job_data_item_table import SingleJobDataItem

from job.job_type import Submit
from db.db_service import dbService


class SubmitService:
    '作业数据投递服务，接收页面调用，把合法的请求转化为记录进行存储'
    
    def save_submit(self, jobDataSubmit:JobDataSubmit):
        '保存用户的作业投递数据'

        files_to_compute = os.listdir(jobDataSubmit.data_dir)
        singleJobDataItems = []
        for file in files_to_compute:
            singleJobDataItems.append(SingleJobDataItem(
                job_total_id = jobDataSubmit.job_total_id,
                data_file = file
            )
            )
        dbService.addItem(jobDataSubmit)
        dbService.addBatchItem(singleJobDataItems)
        return jobDataSubmit

    def all(self):
        List
        return dbService.query_all(JobDataSubmit)


    def fromUserSubmit(self, submit: Submit):
        '从用户传入的作业投递数据构造投递实体与数据库映射'
        dataSubmit = JobDataSubmit(
            job_name = submit.job_name,
            job_total_id = int(round(time.time() * 1000)),
            data_dir = submit.data_dir,
            user_name = submit.user,
            execute_file_path = submit.execute_file_path,
            single_item_allocation = submit.resource_per_item.json(),
            transfer_flag = 'false',
            transfer_state = 'unhandle',
            create_time = '2022-10-14 11:11:41',
            transfer_begin_time='',
            transfer_end_time='',
        )
        return dataSubmit

       