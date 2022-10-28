#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Filename :main.py
@Description :
@Datatime :2022/09/28 14:25:22
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
'''

from slurm_monitor import monitor
from db import dbcontroller
from job import job_controller
import uvicorn
from fastapi import FastAPI
import sys
from pathlib import Path
from utils.scheduler import Scheduler
from job.submit_service import add_job_data_item_scan_job
from slurm_monitor.monitor import add_slurm_monitor_job

file = Path(__file__)
sys.path.append(str(file.parent.parent))

description = """
    manage multi service, split data and submit it to different service
"""

app = FastAPI(
    title="CrossClusterComputing",
    description=description,
    version="0.0.1",
)
scheduler = Scheduler.AsyncScheduler()


app.include_router(monitor.router)
app.include_router(dbcontroller.router)
app.include_router(job_controller.router)

# monitor.register_scheduler(app=app)

@app.get("/")
async def root():
    return {"message": "Welcom To MultiServerManagement"}

@app.on_event("startup")
async def scan():
    '''添加了定时任务数据条目扫描程序'''
    add_slurm_monitor_job(5)
    add_job_data_item_scan_job(5)
    scheduler.start()

@app.get("/stop")
async def stop():
    scheduler.remove_all_jobs()

if __name__ == '__main__':
    uvicorn.run(app='main:app', host="127.0.0.1",
                port=8001, reload=True, debug=True)