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
from typing_extensions import Self

CONFIG = configparser.ConfigParser()

__all__= ["Configuration"]


class Configuration:
    SCONFIG = "service.config"
    service_config_file = Path(__file__).parent.joinpath(SCONFIG)
    CONFIG.read(service_config_file)
    SECS = CONFIG.sections()

    def ServiceConfig():
        for key in Configuration.SECS:
            if key.startswith("slurm"):
                assert "host" in CONFIG[key], f"{key} must have a hostname"
                assert "port" in CONFIG[key], f"{key} must have a port"
                assert "user" in CONFIG[key], f"{key} must have a user"
                assert "password" in CONFIG[key], f"{key} must have a password"
                yield CONFIG[key]
