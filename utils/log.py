#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Filename :log.py
@Description :
@Datatime :2022/10/10 16:47:47
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
'''
from cmath import log
import os
from pathlib import Path
from loguru import logger
import logging

__all__ = ["Log"]

LOGDIR = Path(__file__).parent.parent/"log"


class Log:

    @staticmethod
    def __get_file__(filename):
        if not os.path.exists(LOGDIR):
            os.makedirs(LOGDIR)
        return LOGDIR/filename

    @classmethod
    def log(cls, logfile="job.log", console=False):
        config = {
            "level": logging.INFO,
            "console": False,
            "format": "%(asctime)s-%(filename)s:%(funcName)s[line:%(lineno)d] %(levelname)s %(message)s",
            "datafmt": "%Y-%m-%d %H:%M:%S",
            "filemod": "a"
        }
        logging.basicConfig(
            datefmt=config["datafmt"],
        )
        logger = logging.getLogger()
        logger.setLevel(level=config["level"])
        formatter = logging.Formatter(config["format"])
        print(cls.__get_file__(filename=logfile))
        file_handler = logging.FileHandler(cls.__get_file__(filename=logfile))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        if console:
            console_handler = logging.StreamHandler()
            logger.addHandler(console_handler)
        return logger

    @classmethod
    def ulog(cls, logfile="job.log", console=False):
        if not console:
            logger.remove(handler_id=None)
        format = "{time:YYYY-MM-DD HH:mm:ss} {level} From File_{file} Function_{function} Line_{line} : {message}"
        logger.add(f"{cls.__get_file__(logfile)}", rotation="200MB", encoding="utf-8", enqueue=True,
                   retention="30 days", format=format, compression='zip', level="INFO", filter=lambda record: record["extra"]["name"] == logfile)
        logger_dir = logger.bind(name=logfile)
        return logger_dir
