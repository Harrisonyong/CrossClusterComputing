#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/31 周一 14:49:32
# description: 用于实现与数据库运行作业表进行交互



from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import sys
from pathlib import Path
from typing import List
sys.path.append(str(Path(__file__).parent.parent))
from db.db_service import dbService
from db.dp_running_job_table import RunningJob

engine = create_engine(dbService.dbConfig()["file"], connect_args={
                       "check_same_thread": False})
Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


class DBRunningJobService:
    def add(self, runningJob: RunningJob):
        '新增一条运行作业到数据表中'
        with Session() as session:
            session.add(runningJob)
            
            session.commit()

    def addBatch(self, runningJobs: List[RunningJob]):
        '添加一组运行作业记录到数据库中'
        assert len(runningJobs) > 0, "确保存在插入数据库的作业数据条目"
        dbService.addBatchItem(runningJobs)

    def query_all(self)-> List[RunningJob]:
        '查询作业数据表中的全部记录'
        return dbService.query_all(RunningJob)

    def complete(self, runningJob: RunningJob):
        '把运行作业状态置为完成'

        runningJob.state = "COMPLETED"
        self.add(runningJob)

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

if __name__ == '__main__':
    test()