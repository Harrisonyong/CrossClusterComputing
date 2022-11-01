#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/19 周三 16:07:44
# description: 运行作业表

from datetime import datetime
import string
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine, func, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


class RunningJob(Base):
    """表示运行作业表，其对应slurm中正在运行的作业"""
    __tablename__ = 'dp_running_job_table'

    primary_id = Column(Integer, primary_key=True)

    job_total_id = Column(Integer)
    
    '正在处理的文件名列表'
    file_list = Column(String)
    'slurm脚本路径'
    sbatch_file_path = Column(String)
    '作业运行集群'
    cluster_name = Column(String)
    '作业运行分区'
    partition_name = Column(String)
    '作业id'
    job_id = Column(Integer)

    '作业运行状态'
    state = Column(String)
    '记录创建时间'
    create_time = Column(DateTime, nullable=False, default=datetime.now)
    update_time = Column(DateTime(timezone=True), default = datetime.now, onupdate = datetime.now, comment = "修改时间")
    

    def __repr__(self):
        return "<RunningJob(job_total_id=%s, job_name=%s, create_time=%s))>" % (self.job_total_id, self.job_name, self.create_time)
