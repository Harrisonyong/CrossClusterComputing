#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/13 周四 17:15:17
# description: 该文件负责把用户的作业投递请求创建修改成作业投递记录，存入作业数据投递数据表中


from datetime import datetime
from random import Random

import stat
import time
import os
import threading
from typing import List
from db.db_service import dbService
from db.dp_cluster_status_table import ClusterStatus, PartitionStatus
from db.dp_job_data_submit_table import JobDataSubmit
from db.dp_running_job_table import RunningJob
from db.dp_single_job_data_item_table import SingleJobDataItem
import job
from job.SingleJobDataItemService import singleJobDataItemService
from job.submit_state import SubmitState
from slurm_monitor.serverconn import Connector, SlurmServer
from slurm_monitor.monitor import slurm_search
from utils.date_utils import dateUtils
# from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED

from job.job_type import Submit
from db.db_service import dbService
from job.db_job_submit import dBJobSubmitService
from job.SingleJobDataItemService import singleJobDataItemService
from utils.log import Log
from functools import partial
from db.db_partition import dBPartionService
from db.db_running_job import dbRunningJobService

from utils.scheduler import Scheduler
scheduler = Scheduler.AsyncScheduler()

sbatch_file_path = "D:\\200-Git\\220-slurm\\bash"+os.path.sep+"root"


def handleJobDataItem():
    """执行定期扫描程序，处理所有的作业数据条目"""
    groups = singleJobDataItemService.groupByJobTotalId()
    job_total_ids = [group[0] for group in groups]
    print("时刻{tm}共有{size}类,内容{con}的作业数据条目待处理".format(size=len(groups), con=[group[0] for group in groups], tm=time.strftime('%Y:%m:%d %H:%M:%S',
          time.localtime(int(time.time())))))
    # 待处理的作业条目信息, 此时可以通过策略的不同
    runningRecords = dBJobSubmitService.getSubmitRecords(job_total_ids)
    partions = dBPartionService.get_available_partitions()
    if len(runningRecords) > 0 and len(partions) > 0:
        print("共有待处理作业类型: %d个, 可用分区为: %d" %
              (len(runningRecords), len(partions)))
        schedule(runningRecords, partions)


def schedule(runningSubmitRecords: List[JobDataSubmit], partions: List[PartitionStatus]):
    for record in runningSubmitRecords:

        if not canSchdule(record, partions):
            print("该作业：%d, 作业名称: %s无法被此时的分区列表进行调度" %
                  (record.job_total_id, record.job_name))
            continue
        print("该作业：%d, 作业名称: %s可以被此时的分区列表进行调度" %
              (record.job_total_id, record.job_name))
        schduleSubmitRecord(record, partions)


def canSchdule(record: JobDataSubmit, partions: List[PartitionStatus]):
    """判断该类型的作业是否可以被当前可用的分区列表进行调度"""
    for partion in partions:
        return partion.can_schedule(record)
    return False


def schduleSubmitRecord(record: JobDataSubmit, partitions: List[PartitionStatus]):
    "使用此组分区列表调度该作业投递记录"
    while canSchdule(record, partitions):
        print("处理作业:%s, job_total_id: %d" %
              (record.job_name, record.job_total_id))
        print(
            f"该作业仍有{singleJobDataItemService.countOfItems(record.job_total_id)}个作业条目未处理", )
        availIndex = findAvailablePartion(record, partitions)
        print(f"可用的索引为{availIndex}, 可用分区为: {partitions[availIndex]}")
        handle(record, partitions[availIndex])
        del partitions[availIndex]


def findAvailablePartion(record: JobDataSubmit, partions: List[PartitionStatus]) -> int:
    """找到能够用于处理该作业条目的某个分区，返回可用的分区序号"""
    for index, partion in enumerate(partions):
        if partion.can_schedule(record):
            return index

    raise Exception(
        "在findAvailablePartion中，partions: %s, 无法调度作业: %s" % (partions, record))


def handle(record: JobDataSubmit, partition: PartitionStatus):
    "使用分区partition来处理record类型的作业条目"

    print(f"in handle, partition: {partition}")
    maxNum = partition.number_can_schedule(record)
    jobDataItems = singleJobDataItemService.queryAccordingIdAndLimit(
        record.job_total_id, maxNum)
    if len(jobDataItems) < maxNum:
        print("job_total_id=%s作业已经处理完成, 当前时刻=%s" %
              (record.job_total_id, dateUtils.nowStr()))

    batchFileName = getSlurmBatchFileName(record)
    resourceDescriptor = getResourceDescriptor(record, partition)
    jobDescriptor = getJobDescriptor(record, jobDataItems)

    genrateSlurmBatchFile(batchFileName, resourceDescriptor, jobDescriptor)
    jobId, jobStatus = submitJob(batchFileName, partition)

    # 写入运行作业信息

    dbRunningJobService.add(getRunningJob(
        record, partition, jobDataItems, batchFileName, jobId, jobStatus))

    # 移除作业条目
    ids = [item.primary_id for item in jobDataItems]
    singleJobDataItemService.deleteBatch(ids)
    print(f"删除了{len(jobDataItems)}个作业条目")
    # 触发分区状态修改
    triggerPartitionChange(partition)
    print(
        f"本次调度完成：集群名称={partition.cluster_name}, 分区名={partition.partition_name},调度了{record.job_total_id}的{len(jobDataItems)}个作业条目, 作业数据条目为: {[item.data_file for item in jobDataItems]}")


def getRunningJob(record: JobDataSubmit, partition: PartitionStatus, jobDataItems: List[SingleJobDataItem], batchFileName: str, jobId: int, jobStatus: str):
    """
    根据投递数据、分区信息、脚本名称、作业id、作业状态生成
    """
    job = RunningJob()
    job.cluster_name = partition.cluster_name
    job.partition_name = partition.partition_name
    job.file_list = f"{[item.data_file for item in jobDataItems]}"
    job.job_id = jobId
    job.state = jobStatus
    job.sbatch_file_path = batchFileName
    job.job_total_id = record.job_total_id
    return job


def triggerPartitionChange(partition: PartitionStatus):
    cluster: ClusterStatus = partition.clusterstatus
    host = cluster.ip
    port = cluster.port
    user = cluster.user
    password = cluster.password
    slurm_search(name=partition.cluster_name, host = host, port = port, user = user, password = password)
    print(f"分区状态更新完成")


def submitJob(batchFile: str, partition: PartitionStatus):
    """根据分区信息，使用paramico框架来提交作业
    batchFile: 批处理脚本绝对路径，在集群中路径是统一的一致的
    partion: 作业提交的分区
    """
    with SlurmServer.fromPartition(partition) as slurm:
        stdout, stderr = slurm.sbatch("/root/task.sh")
        result = stdout.read().decode("utf-8")
        if "Submitted batch" not in result:
            raise Exception(f"任务调度失败，slurm脚本为: ${batchFile}, 集群为{partition.cluster_name}, 分区为{partition.partition_name}, 结果为{result}")

        # Submitted batch job 1151
        jobId = int(result.strip("\n").split()[3])

    print(f"调度之后生成作业id为{jobId}")
    return jobId, "R"


def getMaxProcessNum(record: JobDataSubmit, partion: PartitionStatus) -> int:
    """"获取此分区能够处理的record最大能力: 即同时处理record的作业条目数量"""

    return 40


def getSlurmBatchFile(record: JobDataSubmit, partition: PartitionStatus, jobDataitems: List[SingleJobDataItem]) -> str:
    "根据作业和分区以及待处理的作业条目信息生成批处理脚本"

    batchFileName = getSlurmBatchFileName(record)
    resourceDescriptor = getResourceDescriptor(record, partition)
    jobDescriptor = getJobDescriptor(record, jobDataitems)

    genrateSlurmBatchFile(batchFileName, resourceDescriptor, jobDescriptor)
    return batchFileName


def getResourceDescriptor(record: JobDataSubmit, partition: PartitionStatus) -> str:
    print(f"record: {record}, partition: {partition}")
    """获取资源描述信息"""
    resourceStr = "#!/bin/bash" + os.linesep+"#SBATCH -J %s" % (getSlurmBatchCannoialFileName(record))+os.linesep+"#SBATCH -N %d" % (
        partition.nodes_avail)+os.linesep+"#SBATCH -p %s" % (partition.partition_name)+os.linesep
    return resourceStr


def getJobDescriptor(record: JobDataSubmit, jobDataItems: List[SingleJobDataItem]) -> str:
    """
    作业实际执行的命令语句，调用封装好的脚本文件
    执行脚本为run.sh
    则运行过程为
    bash run.sh a1.txt, a2.txt, a3.txt
    """
    exeStatements = "bash " + record.execute_file_path + " "
    for item in jobDataItems:
        exeStatements += item.data_file + " "
    return exeStatements


def getSlurmBatchFileName(record: JobDataSubmit) -> str:
    """根据作业信息和根目录获取批处理作业文件名称"""
    canonicalName = getSlurmBatchCannoialFileName(record)
    return sbatch_file_path + os.path.sep + canonicalName


def getSlurmBatchCannoialFileName(record: JobDataSubmit):
    """
    根据作业投递条目获得batch脚本的名称 经典名称 只有最后的文件名称，不包含其他路径
    """
    return "%s-%s-%s.sh" % (record.job_name, record.job_total_id, dateUtils.jobNowStr())


def getSlurmJobName(record: JobDataSubmit):
    """
    根据作业投递条目获得batch脚本的名称 经典名称 只有最后的文件名称，不包含其他路径
    """
    return "%s-%s-%s" % (record.job_name, record.job_total_id, dateUtils.jobNowStr())


def genrateSlurmBatchFile(absoluteBatchFileName: str, resourceDescriptor: str, jobDescriptor: str):
    """使用文件io操作创建slurm脚本文件,并添加可执行权限"""
    with open(absoluteBatchFileName, 'w') as batchFile:
        batchFile.write(resourceDescriptor)
        batchFile.write(jobDescriptor)
    os.chmod(absoluteBatchFileName, stat.S_IEXEC)
    return


def add_job_data_item_scan_job(interval: int):
    '''添加定时单条数据扫描程序'''
    print("Enter add_job_data_item_scan_job")
    scheduler.add_job(handleJobDataItem, args=[], id=f"single-thread",
                      trigger="interval", seconds=interval, replace_existing=True)
    print("定时扫描任务监控任务启动")


class SubmitService:
    '作业数据投递服务，接收页面调用，把合法的请求转化为记录进行存储'

    def save_submit(self, jobDataSubmit: JobDataSubmit):
        '保存用户的作业投递数据'
        job_total_id = jobDataSubmit.job_total_id
        singleJobDataItems = self.getSingleJobDataItems(jobDataSubmit)
        dbService.addItem(jobDataSubmit)

        # 使用异步线程处理该投递作业相关的数据条目
        threading.Thread(target=self.transfer, args=(
            job_total_id, singleJobDataItems), name="异步转换线程:"+str(job_total_id)).start()

        return jobDataSubmit

    def getSingleJobDataItems(self, jobDataSubmit: JobDataSubmit):
        files_to_compute = os.listdir(jobDataSubmit.data_dir)
        singleJobDataItems = []
        for file in files_to_compute:
            item = SingleJobDataItem()
            item.data_file = os.path.join(jobDataSubmit.data_dir, file)
            item.job_total_id = jobDataSubmit.job_total_id
            singleJobDataItems.append(item)
        assert len(singleJobDataItems) > 0, "数据目录下没有文件！"
        print("作业号：", jobDataSubmit.job_total_id,
              " 共有待处理的数据条目: ", len(singleJobDataItems))
        return singleJobDataItems

    def all(self):
        return dbService.query_all(JobDataSubmit)

    def fromUserSubmit(self, submit: Submit):
        '从用户传入的作业投递数据构造投递实体与数据库映射'
        dataSubmit = JobDataSubmit(
            job_name=submit.job_name,
            job_total_id=int(round(time.time() * 1000)),
            data_dir=submit.data_dir,
            user_name=submit.user,
            execute_file_path=submit.execute_file_path,
            single_item_allocation=submit.resource_per_item.json(),
            transfer_flag=str(False),
            transfer_state=SubmitState.UN_HANDLE.value,
            transfer_begin_time=datetime.now(),
            transfer_end_time=datetime.now(),
        )
        return dataSubmit

    def transfer(self, job_total_id: int, singleJobDataItems: list[SingleJobDataItem]):
        '''转换过程，该函数应该为事务，保持一致性。'''
        '''同时，当前未考虑异常情况，即线程崩溃，若要解决该问题，可以保存线程号'''
        print("异步开始, 处理线程为: ", threading.currentThread().getName(),
              "线程号：", threading.currentThread().native_id)

        # 异步开始
        # 那条记录状态更新为正在处理中
        print("记录更新为正在处理中, 记录批号", job_total_id)
        dBJobSubmitService.updateSubmitRecordHandling(job_total_id)
        dbService.addBatchItem(singleJobDataItems)
        dBJobSubmitService.updateSubmitRecordHandled(job_total_id)
        # 那条记录状态更新为处理完成
        # 异步完成
