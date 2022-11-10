#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/19 周三 11:31:57
# description: 该文件负责周期性的处理作业条目数据，组织成slurm脚本，并通过paramico提交作业

import os
import sys
from pathlib import Path

from utils import Configuration

sys.path.append(str(Path(__file__).parent.parent))
import stat
import time
from typing import List
from db.db_cluster import dBClusterService
from db.db_partition import dBPartitionService
from db.db_running_job import dbRunningJobService
from db.dp_cluster_status_table import ClusterStatus, PartitionStatus
from db.dp_job_data_submit_table import JobDataSubmit
from db.dp_running_job_table import RunningJob
from db.dp_single_job_data_item_table import SingleJobDataItem
from job.SingleJobDataItemService import singleJobDataItemService
from job.db_job_submit import dBJobSubmitService
from job.slurm_job_state import SlurmJobState
from slurm_monitor.monitor import slurm_search
from slurm_monitor.serverconn import SlurmServer
from utils.date_utils import dateUtils

computation_result_path = "D:\\200-Git\\220-slurm\\bash\\output"


def handle_job_data_item():
    """执行定期扫描程序，处理所有的作业数据条目"""
    groups = singleJobDataItemService.groupByJobTotalId()
    job_total_ids = [group[0] for group in groups]
    print("时刻{tm}共有{size}类,内容{con}的作业数据条目待处理".format(size=len(groups), con=[group[0] for group in groups],
                                                     tm=time.strftime('%Y:%m:%d %H:%M:%S',
                                                                      time.localtime(int(time.time())))))
    # 待处理的作业条目信息, 此时可以通过策略的不同
    running_submits = dBJobSubmitService.getSubmitRecords(job_total_ids)
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
        print(f"job_total_id={record.job_total_id}作业已经处理完成, 当前时刻={dateUtils.nowStr()}")

    batch_file_name = get_slurm_batch_file_name(record)
    resource_descriptor = get_resource_descriptor(record, partition)
    job_descriptor = get_job_descriptor(record, job_data_items)

    generate_slurm_batch_file(batch_file_name, resource_descriptor, job_descriptor)

    job_id, job_status = submit_job(batch_file_name, partition)

    # 写入运行作业信息
    dbRunningJobService.add(get_running_job(
        record, partition, job_data_items, batch_file_name, job_id, job_status))
    # 数据不同步问题
    # 移除作业条目
    ids = [item.primary_id for item in job_data_items]
    singleJobDataItemService.deleteBatch(ids)
    print(f"删除了{len(job_data_items)}个作业条目")
    # 触发分区状态修改
    trigger_partition_change(partition)
    print(
        f"本次调度完成：集群名称={partition.cluster_name}, 分区名={partition.partition_name},调度了{record.job_total_id}的{len(job_data_items)}个作业条目, 作业数据条目为: {[item.data_file for item in job_data_items]}")


def get_running_job(record: JobDataSubmit, partition: PartitionStatus, jobDataItems: List[SingleJobDataItem],
                    batch_file_name: str, job_id: int, job_status: str):
    """
    根据投递数据、分区信息、脚本名称、作业id、作业状态生成
    """
    job = RunningJob()
    job.cluster_name = partition.cluster_name
    job.partition_name = partition.partition_name
    job.file_list = f"{[item.data_file for item in jobDataItems]}"
    job.job_id = job_id
    job.state = job_status
    job.sbatch_file_path = batch_file_name
    job.job_total_id = record.job_total_id
    return job


def trigger_partition_change(partition: PartitionStatus):
    cluster: ClusterStatus = partition.clusterstatus
    host = cluster.ip
    port = cluster.port
    user = cluster.user
    password = cluster.password
    slurm_search(name=partition.cluster_name, host=host, port=port, user=user, password=password)
    print(f"分区状态更新完成")


def submit_job(batch_file: str, partition: PartitionStatus):
    """根据分区信息，使用paramiko框架来提交作业
    batch_file: 批处理脚本绝对路径，在集群中路径是统一的一致的
    partition: 作业提交的分区
    """
    cluster = dBClusterService.get_cluster_by_name(partition.cluster_name)
    with SlurmServer.from_cluster(cluster) as slurm:
        stdout, stderr = slurm.sbatch(batch_file)
        result = stdout.read().decode("utf-8")
        if "Submitted batch" not in result:
            raise Exception(
                f"任务调度失败，slurm脚本为: {batch_file}, 集群为{partition.cluster_name}, 分区为{partition.partition_name}, 结果为{result}")

        # Submitted batch job 1151
        job_id = int(result.strip("\n").split()[3])

    print(f"调度之后生成作业id为{job_id}")
    return job_id, SlurmJobState.RUNNING.value


def get_resource_descriptor(record: JobDataSubmit, partition: PartitionStatus) -> str:
    print(f"record: {record}, partition: {partition}")
    """获取资源描述信息"""
    resource_str = "#!/bin/bash" + os.linesep + "#SBATCH -J %s" % (
        get_slurm_batch_canonical_file_name(record)) + os.linesep + "#SBATCH -N %d" % (
                       partition.nodes_avail) + os.linesep + "#SBATCH -p %s" % partition.partition_name + os.linesep
    return resource_str


def get_job_descriptor(record: JobDataSubmit, jobDataItems: List[SingleJobDataItem]) -> str:
    """
    作业实际执行的命令语句，调用封装好的脚本文件
    执行脚本为run.sh
    则运行过程为
    bash run.sh a1.txt, a2.txt, a3.txt
    """
    exe_statements = "bash " + record.execute_file_path + " --input_files="
    exe_statements += ",".join([item.data_file for item in jobDataItems])
    exe_statements += " --output_dir="+computation_result_path
    return exe_statements


def get_slurm_batch_file_name(record: JobDataSubmit) -> str:
    """根据作业信息和根目录获取批处理作业文件名称"""
    canonical_name = get_slurm_batch_canonical_file_name(record)
    sbatch_config = Configuration.sbatch_config()
    return sbatch_config.get_slurm_batch_file_path(record.job_total_id, canonical_name)


def get_slurm_batch_canonical_file_name(record: JobDataSubmit):
    """
    根据作业投递条目获得batch脚本的名称 经典名称 只有最后的文件名称，不包含其他路径
    """
    return "%s-%s-%s.sh" % (record.job_name, record.job_total_id, dateUtils.jobNowStr())


def get_slurm_job_name(record: JobDataSubmit):
    """
    根据作业投递条目获得batch脚本的名称 经典名称 只有最后的文件名称，不包含其他路径
    """
    return "%s-%s-%s" % (record.job_name, record.job_total_id, dateUtils.jobNowStr())


def generate_slurm_batch_file(absolute_batch_file_name: str, resource_descriptor: str, job_descriptor: str):
    """使用文件io操作创建slurm脚本文件,并添加可执行权限"""
    with open(absolute_batch_file_name, 'w') as batch_file:
        batch_file.write(resource_descriptor)
        batch_file.write(job_descriptor)
    os.chmod(absolute_batch_file_name, stat.S_IEXEC)
    return
