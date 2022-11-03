#!/usr/bin/env python
# -*- coding:UTF-8 -*-
# author: songquanheng
# email: wannachan@outlook.com
# date: 2022/10/19 周三 15:54:16
# description: 日期时间类

from datetime import datetime


class DateUtils:
    """日期时间的辅助类"""

    def nowStr(self):
        now = datetime.now()
        return now.strftime('%Y-%m-%d %H:%M:%S')

    def jobNowStr(self):
        now = datetime.now()
        return now.strftime('%Y-%m-%d-%H-%M-%S')


dateUtils = DateUtils()
if __name__ == '__main__':
    print(dateUtils.nowStr())
