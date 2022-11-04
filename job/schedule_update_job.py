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
from db.db_running_job import DBRunningJobService
from db.dp_running_job_table import RunningJob
from db.dp_single_job_data_item_table import SingleJobDataItem
from job.SingleJobDataItemService import singleJobDataItemService
from slurm_monitor.serverconn import SlurmServer
from utils.date_utils import dateUtils
from utils.log import Log

log = Log.ulog("schedule_update_job.log")


def schedule_update_job():
    """周期更新作业的状态"""
    log.info(f"开始更新作业运行状态, 当前时刻为{dateUtils.nowStr()}")
    clusters = DBRunningJobService.query_clusters_has_uncompleted_job()
    handle_clusters(clusters)


def handle_clusters(clusters: List[str]):
    """依次处理每一个集群信息"""
    for cluster in clusters:
        handle(cluster)


def handle(cluster_name: str):
    """处理每一个集群中的所有未处于终止状态的作业"""
    update_running_job_state(cluster_name)
    reschedule(cluster_name)


def reschedule(cluster_name):
    """
    重新还原集中中需要调度的作业组
    @param cluster_name: 集群名称
    """
    jobs_needs_reschedule = DBRunningJobService.query_jobs_needs_reschedule(cluster_name)
    singleJobDataItemService.addBatch(get_corresponding_job_items(jobs_needs_reschedule))
    DBRunningJobService.delete_batch(jobs_needs_reschedule)


def get_corresponding_job_items(jobs: List[RunningJob]) -> List[SingleJobDataItem]:
    """
    获取一组作业对应的单条作业列表
    @param jobs: 待调度的作业信息
    @return: 返回此组作业信息对应的单条作业数据列表
    """
    single_job_data_items = []
    for job in jobs:
        single_job_data_items.extend(get_single_job_data_items(job))
    return single_job_data_items


def get_single_job_data_items(job: RunningJob) -> List[SingleJobDataItem]:
    """
    把一条运行作业记录转化为一组单条作业条目
    @param job: 运行作业记录
    @return: 返回一组单条作业条目
    """
    return [SingleJobDataItem(job.job_total_id, file) for file in get_file_list(job.file_list)]


def get_file_list(file_list_str) -> List[str]:
    """
    获取运行作业中的文件列表
    @param file_list_str: 格式为"['D:\\200-Git\\220-slurm\\msa\\data_dir\\8 (1).ciff', 'D:\\200-Git\\220-slurm\\msa\\data_dir\\8 (10).ciff']"
    @return: 返回对应的list
    """
    return eval(file_list_str)


def update_running_job_state(cluster_name):
    """
    更新该集群中所有上一个时刻状态为RUNNING的作业状态
    @param cluster_name:
    """
    running_jobs = DBRunningJobService.query_running_jobs(cluster_name)
    print(f"一共有{len(running_jobs)}个作业处于运行中")
    ids = [job1.job_id for job1 in running_jobs]
    current_job_states = get_current_job_states(cluster_name, ids)
    print(current_job_states)
    update_running_jobs_state_to_db(current_job_states, running_jobs)


def update_running_jobs_state_to_db(current_job_states, running_jobs):
    """使用最新的作业状态字典更新运行作业记录到数据库中"""
    for job in running_jobs:
        if str(job.job_id) not in current_job_states:
            continue
        job.state = current_job_states[str(job.job_id)]
    DBRunningJobService.addBatch(running_jobs)


def get_current_job_states(cluster_name, ids):
    """
    获取集群中作业的实时状态
    @param cluster_name: 集群名称
    @param ids: 待获取的作业id列表
    @return: 返回字典，其中形如{"12": "COMPLETED", "13", "RUNNING"}
    """
    cluster = dBClusterService.get_cluster_by_name(cluster_name)
    with SlurmServer.from_cluster(cluster) as slurm:
        stdout, stderr = slurm.sacct(ids)
        for _ in range(2):
            next(stdout)

    return {line.split()[0]: line.split()[5] for line in stdout if "." not in line.split()[0]}


