#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/13 周四 16:38:12
# description: 该文件用于与用户进行HTTP交互，以进行作业投递和查看

from asyncio.windows_events import NULL
import sys

from fastapi import APIRouter
from pathlib import Path
from utils.response import Response
from pydantic import BaseModel
sys.path.append(str(Path(__file__).parent.parent))

router = APIRouter(
    prefix="/job-submit",
    tags=["job-submit"],
    responses={404: {"description": "job-submit error"}}
)


class SingleItemAllocation(BaseModel):
    '单个待处理条目所需要的资源'
    node: int
    memory: int
    unit: str

class Submit(BaseModel):
    user: str
    data_dir: str
    execute_file_path: str
    resource_per_item: SingleItemAllocation = NULL



@router.get("/job-submit/welcome")
async def welcome():
   return Response.success(msg="welcome to job-submit page")


@router.post("/job-submit/create")
async def create_submit(submit: Submit):
   return Response.success(data=submit)
