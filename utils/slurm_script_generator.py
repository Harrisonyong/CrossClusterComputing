#!/usr/bin/env python
# -*- coding:UTF-8 -*-

"""
@author: songquanheng
@file: slurm_script_generator.py
@time: 2022/11/11 10:46
@desc: 
"""
import os
import stat


class SlurmScriptGenerator:
    """slurm脚本生成器，用于创建slurm脚本"""
    def __init__(self, script_absolute_path: str):
        self.script_absolute_path = script_absolute_path

    def script_dir_exist(self):
        return os.path.exists(os.path.dirname(self.script_absolute_path))

    def make_script_dir(self):
        """
        为投递创建脚本的根目录 /mnt/ecosystem/.../sbatch-file/1668065975353
        @param absolute_batch_file_name:/mnt/ecosystem/.../sbatch-file/1668065975353/comsoljob-845286.sh
        """
        os.makedirs(os.path.dirname(self.script_absolute_path))

    def generate_script_file(self, resource_descriptor: str, job_descriptor: str):
        """
        生成slurm脚本，首先创建slurm脚本所在的路径，然后创建slurm脚本文件
        @param resource_descriptor:
        @param job_descriptor:
        @return:
        """
        if not self.script_dir_exist():
            self.make_script_dir()

        with open(self.script_absolute_path, 'w') as batch_file:
            batch_file.write(resource_descriptor)
            batch_file.write(job_descriptor)
        os.chmod(self.script_absolute_path, stat.S_IEXEC)








