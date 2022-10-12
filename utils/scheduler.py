#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Filename :scheduler.py
@Description :
@Datatime :2022/10/08 16:05:52
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
'''
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor


__all__ = ["Scheduler"]


class Scheduler:
    sqlite = "sqlite:///" + str(Path(__file__).parent.parent/"data/jobs.db")
    __interval_task = {
        # 配置存储器
        "jobstores": {
            'default': SQLAlchemyJobStore(url=sqlite)
        },
        # 配置执行器
        "executors": {
            'default': ThreadPoolExecutor(20),
            'processpool': ProcessPoolExecutor(5)
        },
        # 创建job时的默认参数
        "job_defaults": {
            'coalesce': True,  # 是否合并执行
            'max_instances': 5,  # 最大实例数
        }

    }

    @staticmethod
    def job_listener(Event, logger, scheduler):
        job = scheduler.get_job(Event.job_id)
        if not Event.exception:
            print('任务正常运行！')
            logger.info(
                f"jobname={job.name};jobid={Event.job_id}|jobtrigger={job.trigger}|jobtime={Event.scheduled_run_time}|retval={Event.retval}")

        else:
            print("任务出错了！！！！！")
            logger.error(
                "jobname={job.name}|jobtrigger={job.trigger}|errcode={Event.code}|exception=[{Event.exception}]|traceback=[{Event.traceback}]|scheduled_time={Event.scheduled_run_time}")

    @classmethod
    def AsyncScheduler(cls):
        return AsyncIOScheduler(**cls.__interval_task)

    @classmethod
    def BackgroundScheduler(cls):
        scheduler = BackgroundScheduler(**cls.__interval_task)
        return scheduler
