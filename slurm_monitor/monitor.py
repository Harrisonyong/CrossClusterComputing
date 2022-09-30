#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Filename :monitor.py
@Description :
@Datatime :2022/09/30 16:37:43
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
'''
import sys
from pathlib import Path
from urllib import response
from fastapi import APIRouter
from ServerConn import SlurmServer
sys.path.append(str(Path(__file__).parent.parent))
from utils.config import Configuration as config

router = APIRouter(
    prefix="/monitor",
    tags = ["monitor"],
    responses={404:{"description":"Not Found any slurm server"}}
)


command = "export PATH=/usr/local/slurm-21.08.8/bin; sinfo"

slurm = SlurmServer(host="10.101.12.48", port=22,
                    user="root", password="szfyd@123")
slurm.exec(command=command)

for conf in config.ServiceConfig():
    print(conf["host"])