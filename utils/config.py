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

CONFIG = configparser.ConfigParser()

__all__= ["Configuration"]


class Configuration:
    SCONFIG = "service.config"
    service_config_file = Path(__file__).parent.joinpath(SCONFIG)
    CONFIG.read(service_config_file)
    SECS = CONFIG.sections()

    def ServiceConfig():
        for name in Configuration.SECS:
            if name.startswith("slurm"):
                assert "host" in CONFIG[name], f"{name} must have a hostname"
                assert "port" in CONFIG[name], f"{name} must have a port"
                assert "user" in CONFIG[name], f"{name} must have a user"
                assert "password" in CONFIG[name], f"{name} must have a password"
                yield (name, CONFIG[name])