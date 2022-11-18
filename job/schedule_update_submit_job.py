#!/usr/bin/env python
# -*- coding:UTF-8 -*-

"""
@author: songquanheng
@file: schedule_update_submit_job.py
@time: 2022/11/17 10:16
@desc: 该文件用于周期性的更新作业提交数
"""
from typing import List

from db.db_cluster import DBClusterService
from db.dp_cluster_status_table import ClusterStatus
from utils.slurm_server import SlurmServer
from utils.date_utils import DateUtils
from utils.log import Log
from utils.slurm_result import SlurmResult

log = Log.ulog("schedule_update_submit_job.log")


def schedule_update_submit_job():
    print('do schedule_update_submit_job time: ', DateUtils.now_str())
    clusters = DBClusterService.get_clusters(0, 1000)
    print(f"当前系统中共有slurm集群{len(clusters)}个")
    update_clusters_submit_job_number(clusters)


def update_clusters_submit_job_number(clusters: List[ClusterStatus]):
    """
    依次更新每个slurm集群，更新其作业数
    @param clusters:集群信息
    @return:
    """
    for cluster in clusters:
        update_cluster_submit_job_number(cluster)


def update_cluster_submit_job_number(cluster: ClusterStatus):
    """
    通过向集群发起slurm请求，获取当前时刻集群中已经提交的作业数
    @param cluster:
    @return:
    """
    with SlurmServer.from_cluster(cluster) as slurm:
        result = SlurmResult(slurm.squeue())
        submit_number = result.get_submit_jobs_number()
        log.info(f"当前时刻{cluster.cluster_name}集群中已经提交了{submit_number}个作业")
        cluster.submit_jobs_num = submit_number
        DBClusterService.save_cluster(cluster)
