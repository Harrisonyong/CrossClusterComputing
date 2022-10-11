#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Description : 该文件用于接收REST请求，并测试程序
@Datatime :2022-10-11 15:42:43
@Author :songquanheng
@email :wannachan@outlook.com
'''
import sys
from datetime import datetime
from pathlib import Path
from urllib import response
from fastapi import APIRouter
sys.path.append(str(Path(__file__).parent.parent))
from utils.config import Configuration as config

router = APIRouter(
    prefix="/db-controller",
    tags=["db-controller"],
    responses={404: {"description": "db error"}}
)

@router.get("/welcome")
async def welcome():
   return {"message": "Welcom To db-controller"}


@router.get("/db-config")
async def dbConfig():
    dbConfig = config.dbConfig()
    return dbConfig
