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
from fastapi import APIRouter
from .serverconn import SlurmServer
sys.path.append(str(Path(__file__).parent.parent))
from utils.config import Configuration as config
from utils.scheduler import Scheduler
from functools import partial
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED
from utils.log import Log


router = APIRouter(
    prefix="/monitor",
    tags=["monitor"],
    responses={404: {"description": "Not Found any slurm server"}}
)

logger = Log.log(logfile="monitor.log")
scheduler = Scheduler.AsyncScheduler()
job_listener = partial(Scheduler.job_listener, logger=logger, scheduler=scheduler)

def slurm_search(host, port, user, password):
    command = "export PATH=/usr/local/slurm-21.08.8/bin; sinfo"
    slurm = SlurmServer(host=host, port=port,user=user, password=password)
    std_out, std_err = slurm.exec(command=command)
    for line in std_out:
        if "idle" and "up" in line:
            info = line.strip("\n").split()
            avail_partition, avail_nodes = info[0], info[3]
            print(f"{host}: avail_partition:{avail_partition}-avail_nodes:{avail_nodes}")
    print(std_err.read().decode("utf8"))
    slurm.close()


@router.get("/run")
async def run():
    scheduler.add_listener(job_listener, EVENT_JOB_ERROR | EVENT_JOB_MISSED | EVENT_JOB_EXECUTED)
    scheduler._logger = logger
    for name, conf in config.ServiceConfig():
        host, port, user, password = conf["host"], conf["port"], conf["user"], conf["password"]
        scheduler.add_job(slurm_search, args=[host, port, user, password], id=f"{name}", trigger="interval", seconds=5, replace_existing=True)
        print(f"定时监控任务{name}启动")
        scheduler.start()
    


@router.get("/stopall")
async def stop():
    scheduler.remove_all_jobs(jobstore=None)