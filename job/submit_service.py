#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/13 周四 17:15:17
# description: 该文件负责把用户的作业投递请求创建修改成作业投递记录，存入作业数据投递数据表中


from datetime import datetime
import time
import os
import threading
from typing import List
from db.db_service import DBService
from db.dp_job_data_submit_table import JobDataSubmit
from db.dp_single_job_data_item_table import SingleJobDataItem
from job.SingleJobDataItemService import singleJobDataItemService
from job.submit_state import SubmitState
from utils.date_utils import dateUtils
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED

from job.job_type import Submit
from db.db_service import dbService
from job.db_job_submit import dBJobSubmitService
from utils.log import Log
from functools import partial

from utils.scheduler import Scheduler
scheduler = Scheduler.AsyncScheduler()
logger = Log.ulog(logfile="single-job-data-item-scan.log")
job_listener = partial(Scheduler.job_listener,
                       logger=logger, scheduler=scheduler)

def handleJobDataItem():
    '''执行定期扫描程序，处理所有的作业数据条目'''
    groups = singleJobDataItemService.groupByJobTotalId()
    singleJobDataItemService.allJobTotalId()
    print(type(groups))
    
    print("时刻{tm}共有{size}类,内容{con}的作业数据条目待处理".format(size=len(groups),con=[group[0] for group in groups], tm=time.strftime('%Y:%m:%d %H:%M:%S',
          time.localtime(int(time.time())))))


def add_job_data_item_scan_job(interval: int):
    '''添加定时单条数据扫描程序'''
    print("Enter add_job_data_item_scan_job")
    scheduler.add_listener(job_listener, EVENT_JOB_ERROR |
                           EVENT_JOB_MISSED | EVENT_JOB_EXECUTED)
    scheduler._logger = logger
    scheduler.add_job(handleJobDataItem, args=[], id=f"single-thread",
                      trigger="interval", seconds=interval, replace_existing=True)
    scheduler.start()
    print("定时扫描任务监控任务启动")


class SubmitService:
    '作业数据投递服务，接收页面调用，把合法的请求转化为记录进行存储'

    def save_submit(self, jobDataSubmit: JobDataSubmit):
        '保存用户的作业投递数据'
        job_total_id = jobDataSubmit.job_total_id
        singleJobDataItems = self.getSingleJobDataItems(jobDataSubmit)
        dbService.addItem(jobDataSubmit)

        # 使用异步线程处理该投递作业相关的数据条目
        threading.Thread(target=self.transfer, args=(
            job_total_id, singleJobDataItems), name="异步转换线程:"+str(job_total_id)).start()

        return jobDataSubmit

    def getSingleJobDataItems(self, jobDataSubmit: JobDataSubmit):
        files_to_compute = os.listdir(jobDataSubmit.data_dir)
        singleJobDataItems = []
        for file in files_to_compute:
            singleJobDataItems.append(SingleJobDataItem(
                job_total_id=jobDataSubmit.job_total_id,
                data_file=file))
        assert len(singleJobDataItems) > 0, "数据目录下没有文件！"
        print("作业号：", jobDataSubmit.job_total_id, " 共有待处理的数据条目: ", len(singleJobDataItems))      
        return singleJobDataItems

    def all(self):
        return dbService.query_all(JobDataSubmit)

    

    def fromUserSubmit(self, submit: Submit):
        '从用户传入的作业投递数据构造投递实体与数据库映射'
        dataSubmit = JobDataSubmit(
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
        return dataSubmit

    def transfer(self, job_total_id: int, singleJobDataItems: list):
        '''转换过程，该函数应该为事务，保持一致性。'''
        '''同时，当前未考虑异常情况，即线程崩溃，若要解决该问题，可以保存线程号'''
        print("异步开始, 处理线程为: ", threading.currentThread().getName(), "线程号：", threading.currentThread().native_id)
        
        # 异步开始
        # 那条记录状态更新为正在处理中
        print("记录更新为正在处理中, 记录批号", job_total_id)
        dBJobSubmitService.updateSubmitRecordHandling(job_total_id)
        dbService.addBatchItem(singleJobDataItems)
        dBJobSubmitService.updateSubmitRecordHandled(job_total_id)
        # 那条记录状态更新为处理完成
        # 异步完成
