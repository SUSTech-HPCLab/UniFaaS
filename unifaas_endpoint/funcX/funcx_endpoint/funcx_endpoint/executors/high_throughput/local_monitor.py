import logging
import threading
import time
from funcx_endpoint.executors.high_throughput.system_info_util import SystemInfoUtil
logger = logging.getLogger(__name__)


class LocalMonitor:

    def __init__(self, monitor_freq=30, record_duration=180):
        self._kill_event = threading.Event()
        self._monitor_thread = threading.Thread(target=self.start,
                                                args=(self._kill_event,),
                                                name="Local-Monitor-Thread")
        self._monitor_thread.daemon = True
        self._monitor_thread.start()
        self.monitor_freq = monitor_freq
        self.record_duration = record_duration
        self.threading_lock = threading.Lock()
        self.info_list = []
        self.max_size = record_duration / monitor_freq

    """
    Starting stage:
        1.gather the basic information of the endpoint. e.g. CPU cores, Memory size, Disk Size
    """

    def start(self, kill_event):
        while not kill_event.is_set():
            info = self.get_system_info()
            with self.threading_lock:
                while len(self.info_list) >= self.max_size > 0:
                    self.info_list.pop(0)
                self.info_list.append(info)
            time.sleep(self.monitor_freq)
        return

    @staticmethod
    def get_system_info():
        cpu_info = SystemInfoUtil.get_current_cpu_info(interval=0.1)
        mem_info = SystemInfoUtil.get_current_mem_info()
        system_info = {**cpu_info, **mem_info}
        return system_info

    def get_avg_info(self):
        with self.threading_lock:
            res = {}
            for d in self.info_list:
                for k, v in d.items():
                    if k in res.keys():
                        res[k] += v
                    else:
                        res[k] = v
            for k in res.keys():
                res[k] = res[k] / len(self.info_list)
        return res
