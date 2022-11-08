#!/usr/bin/env python
# -*- coding:UTF-8 -*-

"""
@author: songquanheng
@file: schedule_service.py
@time: 2022/11/3 15:42
@desc:
"""
from job.schedule_handle_job_data_item import handle_job_data_item
from job.schedule_update_job import schedule_update_job
from slurm_monitor.monitor import add_slurm_monitor_job
# from job.submit_service import handleJobDataItem
from slurm_monitor.monitor import add_slurm_clusters, slurm_search
from utils.scheduler import Scheduler

scheduler = Scheduler.AsyncScheduler()


def add_schedule_service():
    """
    为CrossClusterComputing添加定时调度服务，为时间配置做准备
    """
    add_slurm_monitor_job(5)
    add_schedule_update_job(10)
    add_job_data_item_scan_job(5)


def add_schedule_update_job(interval: int):
    """添加定时单条数据扫描程序"""
    print("Enter add_schedule_update_job")
    scheduler.add_job(schedule_update_job, args=[], id=f"schedule_update_job_thread",
                      trigger="interval", seconds=interval, replace_existing=True)


def add_job_data_item_scan_job(interval: int):
    """添加定时单条数据扫描程序"""
    print("Enter add_job_data_item_scan_job")
    scheduler.add_job(handle_job_data_item, args=[], id=f"job_data_item_scan_job",
                      trigger="interval", seconds=interval, replace_existing=True)
    print("定时扫描任务监控任务启动")