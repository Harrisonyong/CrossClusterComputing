#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
@Description : 该Response类用于统一返回格式
@Datetime :2022-10-13 13:50:56
@Author :songquanheng
@email :wannachan@outlook.com
"""
import json


class Response(object):
    """
    统一的json返回格式
    """

    def __init__(self, code, msg, data) -> None:
        self.code = code
        self.msg = msg
        self.data = data

    @classmethod
    def success(cls, code=0, msg='success', data=None):
        return cls(code, msg, data)

    @classmethod
    def error(cls, code=-1, msg='error', data=None):
        return cls(code, msg, data)

    def to_json(self):
        return json.dumps({"msg": self.msg, "code": self.code, "data": self.data})

    def __str__(self) -> str:
        return self.to_json()


# if __name__ == '__main__':
#     print(Response.success())
#     print(Response.success((1, 2)))
#     print(Response.success(msg="operation success"))
#     print (Response.error())
#     print (Response.error(msg="db insert failed"))
