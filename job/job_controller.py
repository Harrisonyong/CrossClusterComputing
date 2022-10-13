#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/13 周四 16:38:12
# description: 该文件用于与用户进行HTTP交互，以进行作业投递和查看

import sys

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
    submitService.save_submit(submit)
    return Response.success(data=submit)
