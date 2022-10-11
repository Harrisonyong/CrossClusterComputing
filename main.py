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
import uvicorn
from fastapi import FastAPI
import sys
from pathlib import Path

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

app.include_router(monitor.router)


@app.get("/")
async def root():
    return {"message": "Welcom To MultiServerManagement"}

if __name__ == '__main__':
    uvicorn.run(app='main:app', host="127.0.0.1",
                port=8001, reload=True, debug=True)
