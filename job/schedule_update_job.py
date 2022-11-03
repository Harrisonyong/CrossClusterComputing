#!/usr/bin/env python
# -*- coding:UTF-8 -*-

"""
@author: songquanheng
@file: schedule_update_job.py
@time: 2022/11/2 14:17
@desc: 周期性更新作业运行状态
"""
from typing import List

from db.db_cluster import dBClusterService
from db.db_running_job import dbRunningJobService, DBRunningJobService
from slurm_monitor.serverconn import SlurmServer
from utils.log import Log
from utils.scheduler import Scheduler
from utils.date_utils import dateUtils
scheduler = Scheduler.AsyncScheduler()

log = Log.ulog("schedule_update_job.log")


def handle(cluster_name: str):
    """处理每一个集群中的所有未处于终止状态的作业"""
    cluster = dBClusterService.get_cluster_by_name(cluster_name)
    print(f"{cluster}")
    running_jobs = DBRunningJobService.query_running_jobs(cluster_name)
    print(f"一共有{len(running_jobs)}个作业处于运行中")
    current_job_state = {}
    with SlurmServer.from_cluster(cluster) as slurm:
        stdout, stderr = slurm.sacct([job.job_id for job in running_jobs])
        for line in stdout:
            print(line)
    pass


def schedule_update_job():
    """周期更新作业的状态"""
    log.info(f"开始更新作业运行状态, 当前时刻为{dateUtils.nowStr()}")
    clusters = DBRunningJobService.query_clusters_has_uncompleted_job()
    print(clusters)
    handle_clusters(clusters)


def handle_clusters(clusters: List[str]):
    """依次处理每一个集群信息"""
    for cluster in clusters:
        handle(cluster)


def add_schedule_update_job(interval: int):
    """添加定时单条数据扫描程序"""
    print("Enter add_schedule_update_job")
    scheduler.add_job(schedule_update_job, args=[], id=f"schedule_update_job_thread",
                      trigger="interval", seconds=interval, replace_existing=True)