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
from db.db_service import dbService
from db.dp_job_data_submit_table import JobDataSubmit
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from job.submit_state import SubmitState


engine = create_engine(dbService.db_config()["file"], connect_args={
                       "check_same_thread": False})
Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


class DBJobSubmitService:
    """用于负责与数据库交互作业投递记录增删改查"""

    def get_submit_record(self, job_total_id: int) -> JobDataSubmit:
        """根据job_total_id获取作业投递记录"""
        with Session() as session:
            return session.query(JobDataSubmit).filter(JobDataSubmit.job_total_id == job_total_id).first()

    def save_submit_record(self, job_data_submit: JobDataSubmit):
        with Session() as session:
            session.add(job_data_submit)
            session.commit()

    def update_submit_record_handling(self, job_total_id: int):
        """更新投递记录状态为处理中，并同时更新转换开始时间"""
        with Session() as session:
            session.query(JobDataSubmit).filter(
            JobDataSubmit.job_total_id == job_total_id) \
            .update({JobDataSubmit.transfer_state: SubmitState.HANDLING.value,
                JobDataSubmit.transfer_begin_time: datetime.now()}, synchronize_session=False)
            session.commit()

    def update_submit_record_handled(self, job_total_id: int):
        """更新投递记录状态为处理完成，并同时更新转换完成时间"""
        record = self.get_submit_record(job_total_id)
        record.transfer_end_time = datetime.now()
        record.transfer_state = SubmitState.HANDLED.value
        record.transfer_flag = str(True)
        self.save_submit_record(record)

    def get_all_submit_record_order_by_create_time(self)->List[JobDataSubmit]:
        '按顺序获取所有的作业数据投递记录'
        with Session() as session:
            return session.query(JobDataSubmit).order_by(JobDataSubmit.create_time)

    def get_submit_records(self, job_total_ids: List[int]) -> List[JobDataSubmit]:
        """
        根据job_total_id列表获取需要的作业投递记录
        param: job_total_ids 作业投递记录的整体作业号列表
        """
        with Session() as session:
            return session.query(JobDataSubmit).filter(JobDataSubmit.job_total_id.in_(job_total_ids)).order_by(JobDataSubmit.create_time).all()


dBJobSubmitService = DBJobSubmitService()


def test_get_submit_record():
    record = dBJobSubmitService.get_submit_record(1666233358280)
    print(type(record))
    print(record)
    print(record.job_total_id)


def test_get_submit_records_according_to_ids():
    records = dBJobSubmitService.get_submit_records([1666233358280, 1666235599074, 1666235666819, 1666235753230, 1666235901459, 1666235978961, 1666236078066])

    for record in records :
        print(record)


if __name__ == '__main__':
    test_get_submit_records_according_to_ids()
