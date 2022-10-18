#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/13 周四 16:38:12
# description: 该文件用于与用户进行HTTP交互，以进行作业数据投递和查看
import sys
import os

from fastapi import APIRouter
from pathlib import Path

from job.job_type import Submit
from job.submit_service import SubmitService
from utils.response import Response


sys.path.append(str(Path(__file__).parent.parent))

submitService = SubmitService()
router = APIRouter(
    prefix="/job-submit",
    tags=["job-submit"],
    responses={404: {"description": "job-submit error"}}
)


@router.get("/job-submit/welcome")
async def welcome():
   return Response.success(msg="welcome to job-submit page")


@router.post("/job-submit/create")
async def create_submit(submit: Submit):
    print(submit.json())
    assert os.path.exists(submit.data_dir), "not found {} file.".format(submit.data_dir) 
    assert os.path.exists(submit.execute_file_path), "not found {} file.".format(submit.execute_file_path)
    
    jobDataSubmit = submitService.fromUserSubmit(submit)
    print(str(jobDataSubmit))
    submitService.save_submit(jobDataSubmit)
    
    return Response.success(data=submit)

@router.get("/job-sumit/all")
async def all_submit_records():
    '用于查询所有的用户投递记录'
    return Response.success(submitService.all())
