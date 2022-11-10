#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/31 周一 14:49:32
# description: 用于实现与数据库运行作业表进行交互
import sys
from pathlib import Path
from typing import List
from sqlalchemy import distinct, and_
from job.slurm_job_state import SlurmJobState
sys.path.append(str(Path(__file__).parent.parent))
from db.db_service import dbService, Session
from db.dp_running_job_table import RunningJob


class DBRunningJobService:
    def add(self, runningJob: RunningJob):
        """新增一条运行作业到数据表中"""
        with Session() as session:
            session.add(runningJob)
            session.commit()

    @staticmethod
    def delete_batch(running_jobs: List[RunningJob]):
        job_ids = [job.job_id for job in running_jobs]
        with Session() as session:
            session.query(RunningJob).filter(RunningJob.primary_id.in_(job_ids)).delete(synchronize_session=False)
            session.commit()

    @staticmethod
    def addBatch(runningJobs: List[RunningJob]):
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
    def query_jobs_needs_reschedule(cluster_name: str) -> List[RunningJob]:
        with Session() as session:
            return session.query(RunningJob) \
                .filter(
                and_(RunningJob.state.not_in(SlurmJobState.states_end()), RunningJob.cluster_name == cluster_name)) \
                .all()


dbRunningJobService = DBRunningJobService()
