#!/usr/bin/env python
# -*- coding:UTF-8 -*-

"""
@author: songquanheng
@file: job_delivery.py
@time: 2022/11/11 11:26
@desc: 该文件描述一次作业投递
"""
import os
from typing import List

from db.db_cluster import dBClusterService
from db.dp_cluster_status_table import PartitionStatus
from db.dp_job_data_submit_table import JobDataSubmit
from db.dp_single_job_data_item_table import SingleJobDataItem
from job.slurm_job_state import SlurmJobState
from slurm_monitor.serverconn import SlurmServer
from utils import Configuration
from utils.date_utils import dateUtils
from utils.slurm_resource_descriptor import SlurmResourceDescriptor
from utils.slurm_script_generator import SlurmScriptGenerator


class JobDelivery:
    """代表一次作业投递，其包括了作业投递、分区信息、具体投递的作业条目"""

    def __init__(self, submit: JobDataSubmit, partition: PartitionStatus, job_data_items: List[SingleJobDataItem]):
        self.job_data_submit = submit
        self.partition = partition
        self.job_data_items = job_data_items
        self.job_id = None
        self.job_state = SlurmJobState.RUNNING.value
        self.job_name = self.get_slurm_job_name()

        print("in JobDelivery.__init__ before execute")
        self.slurm_script_path = self.get_slurm_batch_file_name()
        print(f"in JobDelivery.__init__ before execute {self.slurm_script_path}")

    def generate_slurm_script(self):
        resource_descriptor = self.get_resource_descriptor()
        job_descriptor = self.get_job_descriptor()
        script_generator = SlurmScriptGenerator(self.slurm_script_path)
        script_generator.generate_script_file(resource_descriptor, job_descriptor)

    def delivery(self):
        """投递的主要过程包括
        1. 产生slurm脚本
        2. 提交作业生成作业id
        """
        self.generate_slurm_script()
        self.submit_job()

    def get_slurm_batch_file_name(self) -> str:
        """根据作业信息和根目录获取批处理作业文件名称"""
        canonical_name = self.get_slurm_batch_canonical_file_name()
        sbatch_dir = Configuration.sbatch_config().get_sbatch_dir()
        return self.get_slurm_batch_file_path(sbatch_dir, canonical_name)

    def get_slurm_batch_canonical_file_name(self):
        """
        根据作业投递条目获得batch脚本的名称 经典名称 只有最后的文件名称，不包含其他路径
        """
        return "%s.sh" % self.job_name

    def get_slurm_job_name(self):
        """
        根据作业投递条目获得batch脚本的名称 经典名称 只有最后的文件名称，不包含其他路径
        """
        return "%s-%s-%s" % (self.job_data_submit.job_name, self.job_data_submit.job_total_id, dateUtils.job_now_str())

    def get_slurm_batch_file_path(self, sbatch_dir: str, batch_file_name: str):
        """该函数用于返回通过整体作业号和slurm脚本名称生成的脚本的整体路径"""
        return os.path.join(sbatch_dir, str(self.job_data_submit.job_total_id), batch_file_name)

    def get_resource_descriptor(self) -> str:
        """获取资源描述信息"""
        print(f"record: {self.job_data_submit}, partition: {self.partition}")

        job_name = self.get_slurm_job_name()
        node_count = self.partition.nodes_avail
        partition_name = self.partition.partition_name
        return SlurmResourceDescriptor.resource_descriptor(job_name, node_count, partition_name, 64)

    def get_job_descriptor(self) -> str:
        """
        作业实际执行的命令语句，调用封装好的脚本文件
        执行脚本为run.sh
        则运行过程为
        bash run.sh a1.txt, a2.txt, a3.txt
        @param self:
        """
        record = self.job_data_submit
        exe_statements = "bash " + record.execute_file_path + " --input_files="
        exe_statements += ",".join([item.data_file for item in self.job_data_items])
        exe_statements += " --output_dir=" + self.job_data_submit.output_dir
        return exe_statements

    def submit_job(self):
        """根据分区信息，使用paramiko框架来提交作业
        batch_file: 批处理脚本绝对路径，在集群中路径是统一的一致的
        partition: 作业提交的分区
        """
        batch_file = self.slurm_script_path
        partition = self.partition
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
        self.job_id = job_id
        self.job_state = SlurmJobState.RUNNING.value
