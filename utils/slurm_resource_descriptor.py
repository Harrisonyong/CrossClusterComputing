#!/usr/bin/env python
# -*- coding:UTF-8 -*-

"""
@author: songquanheng
@file: slurm_resource_descriptor.py
@time: 2022/11/11 11:12
@desc: 用于描述Slurm脚本的资源描述信息
"""
import os


class SlurmResourceDescriptor:
    @staticmethod
    def job_name_desc(job_name: str):
        return "#SBATCH -J %s" % job_name + os.linesep

    @staticmethod
    def nodes_num_desc(node_count: int):
        return "#SBATCH -N %d" % node_count + os.linesep

    @staticmethod
    def partition_desc(partition_name: str):
        return "#SBATCH -p %s" % partition_name + os.linesep

    @staticmethod
    def cpu_num_desc(cpu_num):
        return "#SBATCH -n %d" % cpu_num + os.linesep

    @staticmethod
    def resource_descriptor(job_name: str, node_count: int, partition_name: str, cpu_num: int) -> str:
        return "#!/bin/bash" + os.linesep + \
               SlurmResourceDescriptor.job_name_desc(job_name) + \
               SlurmResourceDescriptor.nodes_num_desc(node_count) + \
               SlurmResourceDescriptor.partition_desc(partition_name) + \
               SlurmResourceDescriptor.cpu_num_desc(cpu_num)

