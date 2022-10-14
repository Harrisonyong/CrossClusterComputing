#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@Filename :config.py
@Description :
@Datatime :2022/09/30 10:01:56
@Author :yangqinglin
@email :yangqinglin@zhejianglab.com
'''
import configparser
from pathlib import Path
from textwrap import indent

__all__= ["Configuration"]

class _Config:
    def __init__(self, config, sec:str) -> None:
        self.sec = sec
        self.secs = {}
        for o in config.options(sec):
            self.secs[o] = config.get(sec, o)
    
    def __getattr__(self, opt):
        try:
            return self.secs[opt]
        except:
            return f"{self.sec} has not attr of {opt}"


class ConfigReader:
    def __init__(self, configFile) -> None:
        self.parser = configparser.ConfigParser()
        self.config_file = Path(__file__).parent.parent.joinpath(configFile)
        self.parser.read(self.config_file)
        self.sections = self.parser.sections()
        self.index = 0

    def config(self, sec):
        assert sec in self.parser.sections(); f"{sec} not in {self.config_file}"
        return _Config(self.parser, sec)


    def __iter__(self):
        return self

    def __next__(self):
        if self.index > len(self.sections) - 1:
            raise StopIteration
        sec = self.sections[self.index]
        self.index += 1
        return sec, _Config(self.parser, sec)


class Configuration:
    
    def ServiceConfig():
        slurms = ConfigReader("config/service.config")
        return slurms

    def dbConfig():
        db = ConfigReader("config/db.ini").config("db")
        return {"host": db.host, "file": db.file}
