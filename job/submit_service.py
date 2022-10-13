#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/13 周四 17:15:17
# description: 该文件负责把用户的作业投递请求创建修改成作业投递记录，存入作业数据投递数据表中

from contextlib import nullcontext
from re import sub

from job.job_type import Submit
from db.dbcontroller import dbConfig



from utils.config import Configuration


class SubmitService:
    '作业数据投递服务，接收页面调用，把合法的请求转化为记录进行存储'
    dbConfig = Configuration.dbConfig()
    def save_submit(self, submit:Submit):
        print("单条作业需要节点数量:", submit.resource_per_item.node)
        print("记录存储成功")