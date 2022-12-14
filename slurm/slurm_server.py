#!/usr/bin/env python
# -*- coding:UTF-8 -*-

"""
@author: songquanheng
@file: slurm_server.py
@time: 2022/11/18 10:44
@desc: 
"""
from typing import List

from db.dp_cluster_status_table import PartitionStatus, ClusterStatus
from slurm_monitor.connector import Connector


class SlurmServer(Connector):
    """
        连接slurm集群，执行集群查询
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def from_partition(cls, partition: PartitionStatus):
        cluster: ClusterStatus = partition.clusterstatus
        host = cluster.ip
        port = cluster.port
        user = cluster.user
        password = cluster.password
        return cls(host, port, user, password)

    @classmethod
    def from_cluster(cls, cluster: ClusterStatus):
        host = cluster.ip
        port = cluster.port
        user = cluster.user
        password = cluster.password
        return cls(host, port, user, password)

    # 运行slurm命令需要配置的环境变量
    slurm_environment= "export PATH=/usr/local/slurm-21.08.8/bin:/software/intel/Oneapi/intelpython/latest/bin:/hpc/software/gcc/bin:/hpc/software/lua/lua/bin:/hpc/software/tcl/bin:/hpc/software/intel/Oneapi/vtune/2022.0.0/bin64:/hpc/software/intel/Oneapi/vpl/2022.1.0/bin:/hpc/software/intel/Oneapi/mpi/2021.6.0//libfabric/bin:/hpc/software/intel/Oneapi/mpi/2021.6.0//bin:/hpc/software/intel/Oneapi/mkl/2022.1.0/bin/intel64:/hpc/software/intel/Oneapi/itac/2021.5.0/bin:/hpc/software/intel/Oneapi/inspector/2022.0.0/bin64:/hpc/software/intel/Oneapi/dpcpp-ct/2022.1.0/bin:/hpc/software/intel/Oneapi/dev-utilities/2021.6.0/bin:/hpc/software/intel/Oneapi/debugger/2021.6.0/gdb/intel64/bin:/hpc/software/intel/Oneapi/compiler/2022.1.0/linux/lib/oclfpga/bin:/hpc/software/intel/Oneapi/compiler/2022.1.0/linux/bin/intel64:/hpc/software/intel/Oneapi/compiler/2022.1.0/linux/bin:/hpc/software/intel/Oneapi/clck/2021.5.0/bin/intel64:/hpc/software/intel/Oneapi/advisor/2022.1.0/bin64:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/usr/local/slurm/sbin:/usr/local/slurm/bin:/software/QuantumEspresso/qe70/bin; "

    def exec(self, command):
        _, std_out, std_err \
            = self.sshClient.exec_command(self.slurm_environment + command)
        return std_out, std_err

    def sinfo(self):
        return self.exec("sinfo")

    def sbatch(self, sbatch_path):
        """使用slurm创建批处理作业"""
        return self.exec("sbatch "+ sbatch_path)

    def sacct(self, job_ids: List[int]):
        """使用sacct命令查询作业的最新状态"""
        # 把[1,2,3]转换为"1,2,3"
        ids_str = ",".join(str(item) for item in job_ids)
        print(f"ids_str={ids_str}")
        return self.exec("sacct -j " + ids_str)

    def squeue(self):
        return self.exec("squeue")

    def close(self):
        self.sshClient.close()

    def __enter__(self):
        return self

    def __exit__(self, exceptionType, exceptionVal, trace):
        if trace:
            print(exceptionType)
            print(exceptionVal)
            print(trace)
        self.close()