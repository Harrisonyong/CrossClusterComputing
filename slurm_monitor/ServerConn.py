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


class Connector:
    def __init__(self, host=None, port=None,
                 user=None, password=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.sshClient = paramiko.SSHClient()
        self.sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        self.sshClient.connect(host, port, user, password)


class SlurmServer(Connector):
    """
        连接slurm集群，执行集群查询
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def exec(self, command):
        std_in, std_out, std_err \
            = self.sshClient.exec_command(command)
        for line in std_out:
            print(line.strip("\n"))
        print(std_err.read().decode("utf8"))
        self.sshClient.close()
