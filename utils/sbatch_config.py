#!/usr/bin/env python
# -*- coding:UTF-8 -*-

"""
@author: songquanheng
@file: sbatch_config.py.py
@time: 2022/11/10 14:47
@desc: 新建配置类，用于存储与脚本有关的信息
"""
import os.path


class SbatchConfig:
    """该配置类用于保存系统生成的slurm脚本的根目录"""
    def __init__(self, sbatch_dir: str):
        self.sbatch_dir = sbatch_dir

    def get_sbatch_dir(self):
        return self.sbatch_dir

    def get_slurm_batch_file_path(self, job_total_id: int, batch_file_name: str):
        """该函数用于返回通过整体作业号和slurm脚本名称生成的脚本的整体路径"""
        return os.path.join(self.get_sbatch_dir(), str(job_total_id), batch_file_name)


