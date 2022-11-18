#!/usr/bin/env python
# -*- coding:UTF-8 -*-

"""
@author: songquanheng
@file: slurm_result.py
@time: 2022/11/16 17:09
@desc: 创建类文件，用于描述通过slurm命令查询与slurm有关的信息
"""
from typing import List

from slurm.slurm_job import SlurmJob


class SlurmResult:
    def __init__(self, result: tuple) -> None:
        self.stdout = result[0]
        self.stderr = result[1]
        super().__init__()

    def get_submit_jobs_number(self):
        """用于查询已经提交给集群的作业数"""
        if not self.success():
            raise Exception(f"squeue查询失败， stderr={self.stderr}")

        return len(self._get_submit_jobs())

    def _get_submit_jobs(self) -> List[SlurmJob]:
        next(self.stdout)
        jobs = []
        for line in self.stdout:
            job_info = line.strip("\n").split()
            print(f"{job_info}")
            assert len(job_info) == 8
            jobs.append(SlurmJob(job_info))
        return jobs

    def success(self) -> bool:
        """
        基于传入的内容判断成功还是失败
        @return:
        """

        return not self.stderr.read().decode("utf-8")
