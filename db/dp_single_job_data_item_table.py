#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/17 周一 14:16:13
# description: 单条作业数据表的数据映射实体类定义
from enum import unique
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
from db.dp_job_data_submit_table import JobDataSubmit



class SingleJobDataItem(Base):
    '单条作业数据表'
    __tablename__ = "dp_single_job_data_item_table"
    primary_id = Column(Integer, primary_key=True)  # 自增id号
    # 整体作业批号
    job_total_id = Column(String(255), ForeignKey(
        JobDataSubmit.job_total_id), index=True)
    # 待处理的数据文件    
    data_file = Column(String)

    def __repr__(self) -> str:
        return f"SingleJobDataItem<primary_id: {self.primary_id}, job_total_id: {self.job_total_id}, data_file: {self.data_file}>"
