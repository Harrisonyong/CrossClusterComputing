
#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/14 Fri 10:51:23
# description: 该模块定义了REST请求使用的参数模型，方便接收用户的请求

from pydantic import BaseModel

class SingleItemAllocation(BaseModel):
    '单个待处理条目所需要的资源'
    node: int
    memory: int
    unit: str

class Submit(BaseModel):
    user: str
    data_dir: str
    execute_file_path: str
    job_name: str
    resource_per_item: SingleItemAllocation