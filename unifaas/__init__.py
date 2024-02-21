"""UniFaaS:Programming across Distributed Cyberinfrastructure with Federated Function Serving

This version is a fork of Parsl v1.3.0-dev, but totally incampatible with Parsl now.

It is just a prototype currently under development.

---------
AUTO_LOGNAME
    Special value that indicates Parsl should construct a filename for logging.
"""
import logging
import os
import platform

from unifaas.version import VERSION
from unifaas.app.app import python_app
from unifaas.config import Config
from unifaas.executors import FuncXExecutor
from unifaas.log_utils import set_stream_logger
from unifaas.log_utils import set_file_logger


from unifaas.dataflow.dflow import DataFlowKernel, DataFlowKernelLoader

import multiprocessing
if platform.system() == 'Darwin':
    multiprocessing.set_start_method('fork', force=True)

__author__ = 'SUSTech HPClab'
__version__ = VERSION

AUTO_LOGNAME = -1

__all__ = [

    # decorators
    'python_app',

    # core
    'Config',
    'DataFlowKernel',

    # logging
    'set_stream_logger',
    'set_file_logger',
    'AUTO_LOGNAME',

    'FuncXExecutor'

]

clear = DataFlowKernelLoader.clear
load = DataFlowKernelLoader.load
dfk = DataFlowKernelLoader.dfk
wait_for_current_tasks = DataFlowKernelLoader.wait_for_current_tasks


logging.getLogger('unifaas').addHandler(logging.NullHandler())

if platform.system() == 'Darwin':
    os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'
