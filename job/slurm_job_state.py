#!/usr/bin/env python
# -*- coding:UTF-8 -*-

"""
@author: songquanheng
@file: slurm_job_state.py
@time: 2022/11/2 16:28
@desc: 
"""
from enum import Enum


class SlurmJobState(Enum):
    BOOT_FAIL = "BOOT_FAIL"
    CANCELLED = "CANCELLED"
    CANCELLED_PLUS = "CANCELLED+"
    COMPLETED = "COMPLETED"
    DEADLINE = "DEADLINE"
    FAILED = "FAILED"
    NODE_FAIL = "NODE_FAIL"
    OUT_OF_MEMORY = "OUT_OF_MEMORY"
    PENDING = "PENDING"
    PREEMPTED="PREEMPTED"
    RUNNING = "RUNNING"
    REQUEUED = "REQUEUED"
    RESIZING = "RESIZING"
    REVOKED = "REVOKED"
    SUSPENDED = "SUSPENDED"
    TIMEOUT = "TIMEOUT"

    @staticmethod
    def needs_update(job_state: str):
        return job_state not in SlurmJobState.states_end()

    @staticmethod
    def states_end():
        """作业正常结束或者取消的状态认为，不需要再重新调度"""
        return [SlurmJobState.CANCELLED.value, SlurmJobState.CANCELLED_PLUS.value, SlurmJobState.COMPLETED.value, SlurmJobState.RUNNING.value]
