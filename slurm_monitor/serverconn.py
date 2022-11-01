#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Filename :monitor.py
@Description :
@Datatime :2022/09/28 14:25:05
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
'''
import paramiko
from db.dp_cluster_status_table import ClusterStatus, PartitionStatus

class Connector:
    def __init__(self, host=None, port=None,
                 user=None, password=None):
        print("Enter Connector.__init__")
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        print(f"self.host= {self.host}, self.port = {self.port}, self.user={self.user}, self.password={self.password}")
        self.sshClient = paramiko.SSHClient()
        self.sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        self.sshClient.connect(host, port, user, password)

    

        
class SlurmServer(Connector):
    """
        连接slurm集群，执行集群查询
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def fromPartition(cls, partition: PartitionStatus):
        cluster: ClusterStatus = partition.clusterstatus
        host = cluster.ip
        port = cluster.port
        user = cluster.user
        password = cluster.password
        return cls(host, port, user, password)

    
    # 运行slurm命令需要配置的环境变量
    slurm_envir="export PATH=/usr/local/slurm-21.08.8/bin:/software/intel/Oneapi/intelpython/latest/bin:/hpc/software/gcc/bin:/hpc/software/lua/lua/bin:/hpc/software/tcl/bin:/hpc/software/intel/Oneapi/vtune/2022.0.0/bin64:/hpc/software/intel/Oneapi/vpl/2022.1.0/bin:/hpc/software/intel/Oneapi/mpi/2021.6.0//libfabric/bin:/hpc/software/intel/Oneapi/mpi/2021.6.0//bin:/hpc/software/intel/Oneapi/mkl/2022.1.0/bin/intel64:/hpc/software/intel/Oneapi/itac/2021.5.0/bin:/hpc/software/intel/Oneapi/inspector/2022.0.0/bin64:/hpc/software/intel/Oneapi/dpcpp-ct/2022.1.0/bin:/hpc/software/intel/Oneapi/dev-utilities/2021.6.0/bin:/hpc/software/intel/Oneapi/debugger/2021.6.0/gdb/intel64/bin:/hpc/software/intel/Oneapi/compiler/2022.1.0/linux/lib/oclfpga/bin:/hpc/software/intel/Oneapi/compiler/2022.1.0/linux/bin/intel64:/hpc/software/intel/Oneapi/compiler/2022.1.0/linux/bin:/hpc/software/intel/Oneapi/clck/2021.5.0/bin/intel64:/hpc/software/intel/Oneapi/advisor/2022.1.0/bin64:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/usr/local/slurm/sbin:/usr/local/slurm/bin:/software/QuantumEspresso/qe70/bin; "
    def exec(self, command):
        _, std_out, std_err \
            = self.sshClient.exec_command(self.slurm_envir+command)
        for line in std_out:
            print 
        return std_out, std_err

    def sinfo(self):
        return self.exec("sinfo")

    def sbatch(self, sbatch_path):
        """使用slurm创建批处理作业"""
        std_out, std_err =  self.exec("sbatch "+ sbatch_path) 
        return std_out, std_err
        


    def close(self):    
        self.sshClient.close()
