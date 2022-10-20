#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/20 周四 10:25:48
# description: 用于与数据库中作业数据投递表进行交互



import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from typing import List
from datetime import datetime
from turtle import update
from db.db_service import dbService
from db.dp_job_data_submit_table import JobDataSubmit
from db.dp_single_job_data_item_table import SingleJobDataItem
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from job.submit_state import SubmitState



engine = create_engine(dbService.dbConfig()["file"], connect_args={
                       "check_same_thread": False})
Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


class DBJobSubmitService:
    '用于负责与数据库交互作业投递记录增删改查'

    def getSubmitRecord(self, job_tatal_id: int) -> JobDataSubmit:
        '''根据job_total_id获取作业投递记录'''
        session = Session()
        result = session.query(JobDataSubmit).filter(
            JobDataSubmit.job_total_id == job_tatal_id).first()
        session.close()
        return result

    def saveSubmitRecord(self, jobDataSubmit: JobDataSubmit):
        session = Session()
        session.add(jobDataSubmit)
        session.commit()
        session.close()

    def updateSubmitRecordHandling(self, job_total_id: int):
        """更新投递记录状态为处理中，并同时更新转换开始时间"""
        session = Session()
        session.query(JobDataSubmit).filter(
            JobDataSubmit.job_total_id == job_total_id).update({JobDataSubmit.transfer_state: SubmitState.HANDLING.value, JobDataSubmit.transfer_begin_time: datetime.now()}, synchronize_session=False)
        session.commit()
        session.close()

    def updateSubmitRecordHandled(self, job_total_id: int):
        """更新投递记录状态为处理完成，并同时更新转换完成时间"""
        record = self.getSubmitRecord(job_total_id)
        record.transfer_end_time = datetime.now()
        record.transfer_state = SubmitState.HANDLED.value
        record.transfer_flag = str(True)
        self.saveSubmitRecord(record)

    def getAllSubmitRecordOrderByCreateTime(self)->List[JobDataSubmit]:
        '按顺序获取所有的作业数据投递记录'
        session = Session()
        records = session.query(JobDataSubmit).order_by(JobDataSubmit.create_time)
        session.close()
        return records

    def getSubmitRecords(self, job_total_ids: List[int]) -> List[JobDataSubmit]:
        """
        根据job_total_id列表获取需要的作业投递记录
        param: job_total_ids 作业投递记录的整体作业号列表
        """
        session = Session()
        records = session.query(JobDataSubmit).filter(JobDataSubmit.job_total_id.in_(job_total_ids)).order_by(JobDataSubmit.create_time)
        session.close()
        return records

dBJobSubmitService = DBJobSubmitService()


def testGetSubmitRecord():
    record = dBJobSubmitService.getSubmitRecord(1666233358280)
    print(type(record))
    print(record)
    print(record.job_total_id)

def testGetSubmitRecordsAccordingToIds():
    records = dBJobSubmitService.getSubmitRecords([1666233358280, 1666235599074, 1666235666819, 1666235753230, 1666235901459, 1666235978961, 1666236078066])
    
    for record in records :
        print(record)


if __name__ == '__main__':
    testGetSubmitRecordsAccordingToIds()
