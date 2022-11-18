#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
@Filename :monitor.py
@Description :
@Datetime :2022/09/28 14:25:05
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
"""

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
