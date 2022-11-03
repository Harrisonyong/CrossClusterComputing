#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/31 周一 14:49:32
# description: 用于实现与数据库运行作业表进行交互
import os
import sys
from pathlib import Path
from typing import List

from sqlalchemy import create_engine, distinct, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from job.slurm_job_state import SlurmJobState

sys.path.append(str(Path(__file__).parent.parent))
from db.db_service import dbService
from db.dp_running_job_table import RunningJob

engine = create_engine(dbService.dbConfig()["file"], connect_args={
    "check_same_thread": False})
Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


class DBRunningJobService:
    def add(self, runningJob: RunningJob):
        """新增一条运行作业到数据表中"""
        with Session() as session:
            session.add(runningJob)
            session.commit()

    def addBatch(self, runningJobs: List[RunningJob]):
        '添加一组运行作业记录到数据库中'
        assert len(runningJobs) > 0, "确保存在插入数据库的作业数据条目"
        dbService.addBatchItem(runningJobs)

    def query_all(self) -> List[RunningJob]:
        '查询作业数据表中的全部记录'
        return dbService.query_all(RunningJob)

    def complete(self, running_job: RunningJob):
        """把运行作业状态置为完成"""
        running_job.state = SlurmJobState.COMPLETED.value
        self.add(running_job)

    @staticmethod
    def query_clusters_has_uncompleted_job() -> List[str]:
        """获取有运行作业的所有集群"""
        with Session() as session:
            result = session.query(distinct(RunningJob.cluster_name)) \
                .filter(RunningJob.state != SlurmJobState.COMPLETED.value).all()
            return [item[0] for item in result]

    @staticmethod
    def query_running_jobs(cluster_name: str) -> List[RunningJob]:
        """根据获取集群中需要更新的作业"""
        with Session() as session:
            return session.query(RunningJob) \
                .filter(and_(RunningJob.state == SlurmJobState.RUNNING.value, RunningJob.cluster_name == cluster_name)) \
                .all()

    @staticmethod
    def query_jobs_needs_reschedule(cluster_name: str):
        with Session() as session:
            return session.query(RunningJob) \
                .filter(
                and_(RunningJob.state.not_in(SlurmJobState.states_end()), RunningJob.cluster_name == cluster_name)) \
                .all()


dbRunningJobService = DBRunningJobService()


def test():
    job = RunningJob()
    job.cluster_name = "slurm1"
    job.partition_name = "allNodes"
    job.job_id = 333
    job.job_total_id = 1
    job.file_list = "[1.txt, 2.txt]"
    job.sbatch_file_path = "D:\\1\\2\\3.txt"
    job.state = "R"
    dbRunningJobService.add(job)
    dbRunningJobService.complete(job)
    print(str(dbRunningJobService.query_clusters_has_running_job()))


if __name__ == '__main__':
    test()
