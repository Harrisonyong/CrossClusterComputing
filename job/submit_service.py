#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/13 周四 17:15:17
# description: 该文件负责把用户的作业投递请求创建修改成作业投递记录，存入作业数据投递数据表中


import time
import json

from db.dbcontroller import dbConfig
from sqlalchemy import and_, create_engine, or_
from sqlalchemy.orm import sessionmaker
from db.dp_job_data_submit_table import JobDataSubmit
from utils.config import Configuration

from job.job_type import Submit

dbConfig = Configuration.dbConfig()

class SubmitService:
    '作业数据投递服务，接收页面调用，把合法的请求转化为记录进行存储'
    
    def save_submit(self, submit:Submit):
        '保存用户的作业投递数据'
        print ("作业名称：", submit.job_name)
        jobDataSubmit=self.fromSubmit(submit)
        print(jobDataSubmit)
        engine=create_engine("sqlite:///"+dbConfig["file"])
        Session = sessionmaker(bind=engine)
        session = Session()
        session.add(jobDataSubmit)
        session.commit()

        
        print("记录存储成功")

    def fromSubmit(self, submit: Submit):
        '从用户传入的作业投递数据构造投递实体与数据库映射'
        print("开始转换")
        print("用户数据", submit.job_name)
        dataSubmit = JobDataSubmit(
            job_name = submit.job_name,
            job_total_id = int(round(time.time() * 1000)),
            data_dir = submit.data_dir,
            user_name = submit.user,
            execute_file_path = submit.execute_file_path,
            single_item_allocation = "{'node': 1}",
            transfer_flag = 'false',
            transfer_state = 'unhandle',
            create_time = '2022-10-14 11:11:41',
            transfer_begin_time='',
            transfer_end_time='',
        )
        
        print("转换结果：", dataSubmit.transfer_flag)
        print("作业批号", dataSubmit.job_total_id)
        return dataSubmit

       