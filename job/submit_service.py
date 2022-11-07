#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/13 周四 17:15:17
# description: 该文件负责把用户的作业投递请求创建修改成作业投递记录，存入作业数据投递数据表中


import os
import threading
import time
from datetime import datetime
from typing import List

from db.db_service import dbService
from db.dp_job_data_submit_table import JobDataSubmit
from db.dp_single_job_data_item_table import SingleJobDataItem
from job.db_job_submit import dBJobSubmitService
from job.job_type import Submit
from job.submit_state import SubmitState
from utils.scheduler import Scheduler

scheduler = Scheduler.AsyncScheduler()


class SubmitService:
    """作业数据投递服务，接收页面调用，把合法的请求转化为记录进行存储"""

    def save_submit(self, jobDataSubmit: JobDataSubmit):
        '保存用户的作业投递数据'
        job_total_id = jobDataSubmit.job_total_id
        singleJobDataItems = self.getSingleJobDataItems(jobDataSubmit)
        dbService.addItem(jobDataSubmit)

        # 使用异步线程处理该投递作业相关的数据条目
        threading.Thread(target=self.transfer, args=(
            job_total_id, singleJobDataItems), name="异步转换线程:" + str(job_total_id)).start()

        return jobDataSubmit

    def getSingleJobDataItems(self, jobDataSubmit: JobDataSubmit):
        files_to_compute = os.listdir(jobDataSubmit.data_dir)
        singleJobDataItems = []
        job_total_id = jobDataSubmit.job_total_id
        for file in files_to_compute:
            data_file = os.path.join(jobDataSubmit.data_dir, file)
            singleJobDataItems.append(SingleJobDataItem(job_total_id, data_file))
        assert len(singleJobDataItems) > 0, "数据目录下没有文件！"
        print("作业号：", job_total_id,
              " 共有待处理的数据条目: ", len(singleJobDataItems))
        return singleJobDataItems

    def all(self):
        return dbService.query_all(JobDataSubmit)

    def from_user_submit(self, submit: Submit):
        """从用户传入的作业投递数据构造投递实体与数据库映射"""
        data_submit = JobDataSubmit(
            job_name=submit.job_name,
            job_total_id=int(round(time.time() * 1000)),
            data_dir=submit.data_dir,
            user_name=submit.user,
            execute_file_path=submit.execute_file_path,
            single_item_allocation=submit.resource_per_item.json(),
            transfer_flag=str(False),
            transfer_state=SubmitState.UN_HANDLE.value,
            transfer_begin_time=datetime.now(),
            transfer_end_time=datetime.now(),
        )
        return data_submit

    def transfer(self, job_total_id: int, single_job_data_items: List[SingleJobDataItem]):
        """转换过程，该函数应该为事务，保持一致性。
        同时，当前未考虑异常情况，即线程崩溃，若要解决该问题，可以保存线程号"""
        print("异步开始, 处理线程为: ", threading.current_thread().name)

        print("记录更新为正在处理中, 记录批号", job_total_id)
        dBJobSubmitService.updateSubmitRecordHandling(job_total_id)
        dbService.addBatchItem(single_job_data_items)
        dBJobSubmitService.updateSubmitRecordHandled(job_total_id)
