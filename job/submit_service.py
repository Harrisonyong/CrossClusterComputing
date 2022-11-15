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


class SubmitService:
    """作业数据投递服务，接收页面调用，把合法的请求转化为记录进行存储"""

    @staticmethod
    def save_submit(job_data_submit: JobDataSubmit):
        """保存用户的作业投递数据"""
        job_total_id = job_data_submit.job_total_id
        single_job_data_items = SubmitService.get_single_job_data_items(job_data_submit)
        dbService.add_item(job_data_submit)

        # 使用异步线程处理该投递作业相关的数据条目
        threading.Thread(target=SubmitService.transfer, args=(
            job_total_id, single_job_data_items), name="异步转换线程:" + str(job_total_id)).start()

    @staticmethod
    def get_single_job_data_items(job_data_submit: JobDataSubmit):
        job_total_id = job_data_submit.job_total_id
        single_job_data_items = SubmitService.extract_job_data_items(job_data_submit)
        assert len(single_job_data_items) > 0, "数据目录下没有文件！"
        print("作业号：", job_total_id,
              " 共有待处理的数据条目: ", len(single_job_data_items))
        return single_job_data_items

    @staticmethod
    def extract_job_data_items(job_data_submit):
        files_to_compute = os.listdir(job_data_submit.data_dir)
        single_job_data_items = []
        job_total_id = job_data_submit.job_total_id
        for file in files_to_compute:
            data_file = os.path.join(job_data_submit.data_dir, file)
            single_job_data_items.append(SingleJobDataItem(job_total_id, data_file))
        return single_job_data_items

    def all(self):
        return dbService.query_all(JobDataSubmit)

    @staticmethod
    def from_user_submit(submit: Submit):
        """从用户传入的作业投递数据构造投递实体与数据库映射"""
        data_submit = JobDataSubmit(
            job_name=submit.job_name,
            job_total_id=int(round(time.time() * 1000)),
            data_dir=submit.data_dir,
            output_dir = submit.output_dir,
            user_name=submit.user,
            execute_file_path=submit.execute_file_path,
            single_item_allocation=submit.resource_per_item.json(),
            transfer_flag=str(False),
            transfer_state=SubmitState.UN_HANDLE.value,
            transfer_begin_time=datetime.now(),
            transfer_end_time=datetime.now(),
        )
        return data_submit

    @staticmethod
    def transfer(job_total_id: int, single_job_data_items: List[SingleJobDataItem]):
        """转换过程，该函数应该为事务，保持一致性。
        同时，当前未考虑异常情况，即线程崩溃，若要解决该问题，可以保存线程号"""
        print("异步转换投递数据成单条作业条目开始, 处理线程为: ", threading.current_thread().name)
        print("记录更新为正在处理中, 记录批号", job_total_id)
        dBJobSubmitService.update_submit_record_handling(job_total_id)
        dbService.add_batch_item(single_job_data_items)
        dBJobSubmitService.update_submit_record_handled(job_total_id)
        print("异步转换投递数据成单条作业条目结束, 处理线程为: ", threading.current_thread().name)
