#!/usr/bin/env python
# -*- coding:UTF-8 -*-

"""
@author: songquanheng
@file: schedule_service.py
@time: 2022/11/3 15:42
@desc:
"""
from job.schedule_handle_job_data_item import handle_job_data_item
from job.schedule_update_job import schedule_update_job_state
from job.schedule_update_submit_job import schedule_update_submit_job
from slurm_monitor.monitor import add_slurm_clusters, slurm_search
from utils.scheduler import Scheduler

scheduler = Scheduler.AsyncScheduler()


def add_schedule_service():
    """
    为CrossClusterComputing添加定时调度服务，为时间配置做准备
    """
    add_slurm_monitor_job(5)
    add_schedule_update_job(5)
    add_job_data_item_scan_job(5)
    add_slurm_submit_job_number_update_job(5)


def add_schedule_update_job(interval: int):
    """添加定时单条数据扫描程序"""
    print("Enter add_schedule_update_job")
    scheduler.add_job(schedule_update_job_state, args=[], id=f"schedule_update_job_thread",
                      trigger="interval", seconds=interval, replace_existing=True)


def add_job_data_item_scan_job(interval: int):
    """添加定时单条数据扫描程序"""
    print("Enter add_job_data_item_scan_job")
    scheduler.add_job(handle_job_data_item, args=[], id=f"job_data_item_scan_job",
                      trigger="interval", seconds=interval, replace_existing=True)
    print("定时扫描任务监控任务启动")


def add_slurm_monitor_job(interval: int):
    """
        循环将不同的集群分区监控添加到定时任务中
    """
    clusters = add_slurm_clusters()
    for cluster in clusters:
        scheduler.add_job(slurm_search, args=[
            cluster.cluster_name, cluster.ip, cluster.port, cluster.user, cluster.password], id=f"{cluster.cluster_name}", trigger="interval", seconds=interval, replace_existing=True)
        print(f"定时监控任务{cluster.cluster_name}启动")


def add_slurm_submit_job_number_update_job(interval: int):
    """
    添加定期更新所有集群的作业提交数,该实现会把所有的集群在一次调度时进行更新
    @return:
    """
    scheduler.add_job(schedule_update_submit_job, args=[], id=f"schedule_update_job_submit_number_thread",
                      trigger="interval", seconds=interval, replace_existing=True)