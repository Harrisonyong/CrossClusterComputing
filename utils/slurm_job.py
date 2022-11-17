#!/usr/bin/env python
# -*- coding:UTF-8 -*-

"""
@author: songquanheng
@file: slurm_job.py
@time: 2022/11/17 9:32
@desc:
"""


class SlurmJob:
    """映射slurm作业的输出
      5141  allNodes ji-scien     root PD       0:00      2 (Resources)
      5144  allNodes ji-scien     root PD       0:00      2 (Priority)
      5147  allNodes ji-scien     root PD       0:00      1 (Priority)
      5135  allNodes msa_job-     root  R      44:13      2 slurm-compute-1,slurm-manager
    """
    def __init__(self, job_info: list) -> None:
        self.job_id = job_info[0]
        self.partition = job_info[1]
        self.name = job_info[2]
        self.user = job_info[3]
        self.st = job_info[4]
        self.time = job_info[5]
        self.nodes = job_info[6]
        self.node_list = job_info[7]
        super().__init__()

