#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/19 周三 17:40:50
# description: 投递记录的状态

from enum import Enum
class SubmitState(Enum):
    UN_HANDLE = "unhandle"
    HANDLING = "handling"
    HANDLED  = "handled"

if __name__ == '__main__':
    print (SubmitState.HANDLED.value)