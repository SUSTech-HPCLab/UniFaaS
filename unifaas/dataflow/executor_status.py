import unifaas

from abc import ABCMeta, abstractmethod
from typing import Dict


class ExecutorStatus(metaclass=ABCMeta):
    @property
    @abstractmethod
    def executor(self) -> "unifaas.executors.base.UniFaaSExecutor":
        pass

    @property
    @abstractmethod
    def status(self) -> Dict[str, "unifaas.providers.provider_base.JobStatus"]:
        pass
