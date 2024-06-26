import logging
import threading
from itertools import compress
from abc import abstractmethod, abstractproperty
from concurrent.futures import Future
from typing import List, Any, Dict, Optional, Tuple, Union

import unifaas  # noqa F401
from unifaas.executors.base import UniFaaSExecutor
from unifaas.executors.errors import ScalingFailed


logger = logging.getLogger("unifaas")


class NoStatusHandlingExecutor(UniFaaSExecutor):
    def __init__(self):
        super().__init__()
        self._tasks = {}  # type: Dict[object, Future]

    @property
    def status_polling_interval(self):
        return -1

    @property
    def bad_state_is_set(self):
        return False

    @property
    def error_management_enabled(self):
        return False

    @property
    def executor_exception(self):
        return None

    def set_bad_state_and_fail_all(self, exception: Exception):
        pass

    def status(self):
        return {}

    @property
    def tasks(self) -> Dict[object, Future]:
        return self._tasks

    @property
    def provider(self):
        return self._provider
