#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/19 周三 11:31:57
# description: 该文件负责周期性的处理作业条目数据，组织成slurm脚本，并通过paramico提交作业

import sys
from pathlib import Path

from db.db_cluster import dBClusterService
from job.job_delivery import JobDelivery
from utils.log import Log

sys.path.append(str(Path(__file__).parent.parent))
import time
from typing import List
from db.db_partition import dBPartitionService
from db.db_running_job import dbRunningJobService
from db.dp_cluster_status_table import ClusterStatus, PartitionStatus
from db.dp_job_data_submit_table import JobDataSubmit
from db.dp_running_job_table import RunningJob
from job.SingleJobDataItemService import singleJobDataItemService
from job.db_job_submit import dBJobSubmitService
from slurm_monitor.monitor import slurm_search
from utils.date_utils import DateUtils

log = Log.ulog("schedule_handle_job_data_item.log")


def handle_job_data_item():
    """执行定期扫描程序，处理所有的作业数据条目"""
    groups = singleJobDataItemService.groupByJobTotalId()
    job_total_ids = [group[0] for group in groups]
    print("时刻{tm}共有{size}类,内容{con}的作业数据条目待处理".format(size=len(groups), con=[group[0] for group in groups],
                                                     tm=time.strftime('%Y:%m:%d %H:%M:%S',
                                                                      time.localtime(int(time.time())))))
    # 待处理的作业条目信息, 此时可以通过策略的不同
    running_submits = dBJobSubmitService.get_submit_records(job_total_ids)
    partitions = dBPartitionService.get_available_partitions()
    if len(running_submits) <= 0 or len(partitions) <= 0:
        return
    print(f"共有待处理作业类型: {len(running_submits)}个, 可用分区为: {len(partitions)}")
    schedule(running_submits, partitions)


def schedule(running_submit_records: List[JobDataSubmit], partitions: List[PartitionStatus]):
    for submit in running_submit_records:
        if not can_schedule(submit, partitions):
            print("该作业：%d, 作业名称: %s无法被此时的分区列表进行调度" %
                  (submit.job_total_id, submit.job_name))
            continue
        print("该作业：%d, 作业名称: %s可以被此时的分区列表进行调度" %
              (submit.job_total_id, submit.job_name))
        schedule_submit_record(submit, partitions)


def can_schedule(record: JobDataSubmit, partitions: List[PartitionStatus]):
    """判断该类型的作业是否可以被当前可用的分区列表进行调度"""
    for partition in partitions:
        if partition.can_schedule(record):
            return True
    return False


def schedule_submit_record(record: JobDataSubmit, partitions: List[PartitionStatus]):
    """使用此组分区列表调度该作业投递记录"""
    # 由于一次循环可能无法将record类的所有单条数据全部调度完，因此使用while循环
    while can_schedule(record, partitions):
        print(f"处理作业:{record.job_name}, job_total_id: {record.job_total_id:d}")
        print(f"该作业仍有{singleJobDataItemService.countOfItems(record.job_total_id)}个作业条目未处理")
        avail_index = find_available_partition(record, partitions)
        print(f"可用的索引为{avail_index}, 可用分区为: {partitions[avail_index]}")
        handle(record, partitions[avail_index])
        del partitions[avail_index]


def find_available_partition(record: JobDataSubmit, partitions: List[PartitionStatus]) -> int:
    """找到能够用于处理该作业条目的某个分区，返回可用的分区序号"""
    for index, partition in enumerate(partitions):
        if partition.can_schedule(record):
            return index

    raise Exception(
        "在find_available_partition中，partitions: %s, 无法调度作业: %s" % (partitions, record))


def handle(record: JobDataSubmit, partition: PartitionStatus):
    """使用分区partition来处理record类型的作业条目"""

    print(f"in handle, partition: {partition}")
    max_schedule_num = partition.number_can_schedule(record)
    job_data_items = singleJobDataItemService.queryAccordingIdAndLimit(
        record.job_total_id, max_schedule_num)
    if len(job_data_items) < max_schedule_num:
        print(f"job_total_id={record.job_total_id}作业已经处理完成, 当前时刻={DateUtils.now_str()}")

    cluster = dBClusterService.get_cluster_by_name(partition.cluster_name)
    job_delivery = JobDelivery(cluster, partition, record, job_data_items)

    if not job_delivery.can_submit():
        log.info(f"{cluster.cluster_name}集群中已经提交的作业数为{cluster.submit_jobs_num}>={cluster.max_submit_jobs_limit}")
        return

    print(f"{cluster.cluster_name}集群中已经提交的作业数为{cluster.submit_jobs_num}")
    job_delivery.delivery()
    # 写入运行作业信息
    dbRunningJobService.add(get_running_job(job_delivery))
    # 数据不同步问题
    # 移除作业条目
    single_item_primary_ids = [item.primary_id for item in job_delivery.job_data_items]
    singleJobDataItemService.deleteBatch(single_item_primary_ids)
    print(f"删除了{len(job_data_items)}个作业条目")
    # 触发分区状态修改
    trigger_partition_change(partition)
    print(
        f"调度完成：集群名称={partition.cluster_name}, 分区名={partition.partition_name},调度了{record.job_total_id}的{len(job_data_items)}个作业条目, 作业数据条目为: {[item.data_file for item in job_data_items]}")


def get_running_job(job_delivery: JobDelivery):
    """
    根据投递数据、分区信息、脚本名称、作业id、作业状态生成
    @param job_delivery:
    """
    job = RunningJob()
    job.cluster_name = job_delivery.partition.cluster_name
    job.partition_name = job_delivery.partition.partition_name
    job.file_list = f"{[item.data_file for item in job_delivery.job_data_items]}"
    job.job_id = job_delivery.job_id
    job.state = job_delivery.job_state
    job.sbatch_file_path = job_delivery.slurm_script_path
    job.job_total_id = job_delivery.job_data_submit.job_total_id
    job.job_name = job_delivery.job_name
    return job


def trigger_partition_change(partition: PartitionStatus):
    cluster: ClusterStatus = partition.clusterstatus
    host = cluster.ip
    port = cluster.port
    user = cluster.user
    password = cluster.password
    slurm_search(name=partition.cluster_name, host=host, port=port, user=user, password=password)
    print(f"分区状态更新完成")
