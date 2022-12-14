#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Filename :main.py
@Description :
@Datatime :2022/09/28 14:25:22
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
'''

import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from db import db_service as database
from db import db_controller
from job import job_controller
from slurm_monitor import monitor
from utils.schedule_service import add_schedule_service
from utils.scheduler import Scheduler

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
database.Base.metadata.create_all(bind=database.engine)

app.include_router(monitor.router)
app.include_router(db_controller.router)
app.include_router(job_controller.router)


@app.get("/")
async def root():
    return {"message": "Welcom To MultiServerManagement"}


@app.on_event("startup")
async def scan():
    """添加了定时任务数据条目扫描程序"""
    print("启动应用程序")
    add_schedule_service()
    scheduler.start()


@app.on_event("shutdown")
async def shutdown_evnet():
    print("关闭应用程序")
    scheduler.remove_all_jobs()


@app.get("/stop")
async def stop():
    scheduler.remove_all_jobs()

if __name__ == '__main__':
    uvicorn.run(app='main:app', host="0.0.0.0",
                port=8088, reload=True, debug=True)
