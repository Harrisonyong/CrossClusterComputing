#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/19 周三 16:07:44
# description: 作业数据投递表的概览数据


from datetime import datetime
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine, func, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


class JobDataSubmit(Base):
    """表示作业数据投递的实体，与作业数据投递表映射"""
    __tablename__ = 'dp_job_data_submit_table'

    primary_id = Column(Integer, primary_key=True)
    '用户名'
    user_name = Column(String)
    '整体作业投递批号'
    job_total_id = Column(Integer)
    '作业名称'
    job_name = Column(String)
    '待处理数据目录'
    data_dir = Column(String)
    '执行文件目录'
    execute_file_path = Column(String)
    '单条执行耗费资源，使用json'
    single_item_allocation = Column(String)
    '记录创建时间'
    create_time = Column(DateTime, nullable=False, default=datetime.now)

    '是否已经转化, 转化指的是把批处理的所有作业条目存储数据库中'
    transfer_flag = Column(String)
    '转化状态'
    transfer_state = Column(String)
    '转化开始时间'
    transfer_begin_time = Column(DateTime)
    '转化结束时间'
    transfer_end_time = Column(DateTime)
    
    

    def __repr__(self):
        return "<JobDataSubmit(job_total_id=%s, job_name=%s, create_time=%s))>" % (self.job_total_id, self.job_name, self.create_time)

    def nodesNeeded(self) -> float:
        """
        返回该作业投递需要的节点数量,该值为大于0的浮点数
        倘若一个节点可以同时处理4个作业条目，则此作业条目的node值取值为1/4
        """

        
        resource = eval(self.single_item_allocation)
        
        assert resource["node"] > 0
        return resource["node"]