import logging
import concurrent.futures
from unifaas.executors.funcx.executor import FuncXExecutor
import threading
import time

logger = logging.getLogger("unifaas")
import threading

exp_logger = logging.getLogger("experiment")
import pickle
import os
import pathlib

UNIFAAS_HOME = os.path.join(pathlib.Path.home(), ".unifaas")


class ResourceStatusPoller(object):
    """
    1.Poll the status of executor periodically
    2.Record the status of executor, store some info like idle_workers, pending_tasks, etc.
    submit counter is not finished.
    """

    __instance = None
    __first_init = False

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = object.__new__(cls)
        return cls.__instance

    def __init__(self, executors, poll_period=10):
        if ResourceStatusPoller.__first_init == False:
            self.executors = executors
            self.not_changed_total_workers = {}
            self.poll_period = poll_period
            self._stop = False
            self._poller_thread = None
            self.resource_status = {}
            self.real_time_status = {}
            self.cur_being_executed_time = {}
            self._update_resource()
            self._kill_event = threading.Event()
            self._status_poller_thread = threading.Thread(
                target=self.update_from_webserivce,
                args=(self._kill_event,),
                name="Status-Poller-Thread",
            )
            self._status_poller_thread.daemon = True
            self._status_poller_thread.start()
            self._status_monitor_thread = threading.Thread(
                target=self.monitor_resource,
                args=(self._kill_event,),
                name="Status-Monitor-Thread",
            )
            self._status_monitor_thread.daemon = True
            self._status_monitor_thread.start()
            logger.info(f"[ResourceStatusPoller] Start StatePoller")
            ResourceStatusPoller.__first_init = True

    def update_from_webserivce(self, kill_event, interval=10):
        while not kill_event.is_set():
            # break  only for test
            self._update_resource()
            time.sleep(60)

    def _update_resource(self):
        """
        Initialize the resource info by funcx_client feching the resource status
        """
        futures_dict = {}  # {executor : future}
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as thread_executor:
            for funcx_executor in self.executors:
                if isinstance(funcx_executor, FuncXExecutor):
                    futures_dict[funcx_executor.label] = (
                        None
                        if funcx_executor.fxc is None
                        else thread_executor.submit(
                            funcx_executor.fxc.get_endpoint_status,
                            funcx_executor.endpoint,
                        )
                    )

        for executor in futures_dict.keys():
            try:
                data = (
                    futures_dict[executor].result()
                    if futures_dict[executor] is not None
                    else None
                )
            except Exception as e:
                logger.warning(
                    f"Can't get Executor {executor} status due to exception {e} at result"
                )
                continue
            if data is None:
                raise Exception(f"Can't get Executor {executor} status")

            if data.get("status") != "online":
                raise Exception(f"Executor {executor} is not online")
            if data.get("logs") is not None:
                logs = data.get("logs")
                if len(logs) > 0:
                    info = logs[0].get("info")
                    info["max_total_workers"] = info["ic_total_workers"]
                    info["total_workers"] = info["total_workers"]
                    self.not_changed_total_workers[executor] = info["total_workers"]

                    self.resource_status[executor] = info
                    if not executor in self.real_time_status.keys():
                        self.real_time_status[executor] = {
                            "closed": True,
                            "pre_scale": False,
                            "total_workers": 0,
                            "idle_workers": 0,
                            "pending_tasks": 0,
                            "submit_counter": 0,
                            "value_lock": threading.Lock(),
                            "max_blocks": info["max_blocks"],
                            "max_total_workers": info["max_total_workers"],
                            "launch_queue_task_num": 0,
                        }

                    if not executor in self.cur_being_executed_time.keys():
                        self.cur_being_executed_time[executor] = 0
                    exp_logger.info(
                        f"update_from_service {executor}|{info['total_workers']}|{info['idle_workers']}|{info['pending_tasks']}"
                    )
                    if info["total_workers"] == 0:
                        self.real_time_status[executor]["closed"] = True
                        self.real_time_status[executor]["total_workers"] = 0
                        self.real_time_status[executor]["idle_workers"] = 0
                        self.real_time_status[executor]["pending_tasks"] = 0
                        self.real_time_status[executor]["submit_counter"] = 0
                        self.real_time_status[executor]["launch_queue_task_num"] = 0
                    else:
                        if self.real_time_status[executor]["closed"]:
                            self.update_real_time_status_when_open(executor, info)
                        if (
                            info["total_workers"]
                            > self.real_time_status[executor]["total_workers"]
                        ):
                            self.update_when_scaling_out(
                                executor, info["total_workers"]
                            )
                        elif (
                            info["total_workers"]
                            < self.real_time_status[executor]["total_workers"]
                        ):
                            self.update_when_scaling_in(executor, info["total_workers"])
                    exp_logger.info(
                        f"[RealTimeWorkerInfo] {executor}|{self.real_time_status[executor]['total_workers']}|{self.real_time_status[executor]['idle_workers']}|{self.real_time_status[executor]['pending_tasks']}|{self.real_time_status[executor]['launch_queue_task_num']}"
                    )

        self._cal_ideal_upper_bound_of_workers()

    def update_real_time_status_when_closed(self, executor):
        self.real_time_status[executor]["closed"] = True
        self.real_time_status[executor]["total_workers"] = 0
        self.real_time_status[executor]["idle_workers"] = 0
        self.real_time_status[executor]["pending_tasks"] = 0
        self.real_time_status[executor]["submit_counter"] = 0
        self.real_time_status[executor]["launch_queue_task_num"] = 0
        exp_logger.info(
            f"[RealTimeWorkerInfo] close {executor}|{self.real_time_status[executor]['total_workers']}|{self.real_time_status[executor]['idle_workers']}|{self.real_time_status[executor]['pending_tasks']}|{self.real_time_status[executor]['launch_queue_task_num']}"
        )

    def update_real_time_status_when_open(self, executor, info):
        self.real_time_status[executor]["closed"] = False
        self.real_time_status[executor]["pre_scale"] = False
        self.real_time_status[executor]["total_workers"] = info["total_workers"]
        self.real_time_status[executor]["idle_workers"] = info["total_workers"]
        self.real_time_status[executor]["pending_tasks"] = 0
        exp_logger.info(
            f"[RealTimeWorkerInfo] open {executor}|{self.real_time_status[executor]['total_workers']}|{self.real_time_status[executor]['idle_workers']}|{self.real_time_status[executor]['pending_tasks']}|{self.real_time_status[executor]['launch_queue_task_num']}"
        )
        self.real_time_status[executor]["idle_workers"] = max(
            0,
            self.real_time_status[executor]["total_workers"]
            - self.real_time_status[executor]["submit_counter"],
        )
        self.real_time_status[executor]["pending_tasks"] = max(
            0, self.real_time_status[executor]["submit_counter"] - info["total_workers"]
        )
        self.real_time_status[executor]["submit_counter"] = 0
        exp_logger.info(
            f"[RealTimeWorkerInfo] handle counter {executor}|{self.real_time_status[executor]['total_workers']}|{self.real_time_status[executor]['idle_workers']}|{self.real_time_status[executor]['pending_tasks']}|{self.real_time_status[executor]['launch_queue_task_num']}"
        )

    def update_when_launch_task(self, executor, task_record=None):
        self.real_time_status[executor]["launch_queue_task_num"] -= 1
        if (
            self.real_time_status[executor]["pending_tasks"] > 0
            or self.real_time_status[executor]["idle_workers"] <= 0
        ):
            self.real_time_status[executor]["pending_tasks"] += 1
        else:
            self.real_time_status[executor]["idle_workers"] -= 1

    def release_being_executed_time(self, executor, task_record):
        if "predict_execution" in task_record.keys():
            self.cur_being_executed_time[executor] -= task_record["predict_execution"][
                executor
            ]

    def update_status_when_submit_one_task(self, executor, task_record=None):
        if task_record is not None:
            self.cur_being_executed_time[executor] += task_record["predict_execution"][
                executor
            ]

        if self.real_time_status[executor]["closed"]:
            self.real_time_status[executor]["submit_counter"] += 1
        else:
            task_str = ""
            if task_record:
                task_str = f"Task {task_record['id']}"
            self.real_time_status[executor]["launch_queue_task_num"] += 1
            total_workers = self.real_time_status[executor]["total_workers"]
            idle_workers = self.real_time_status[executor]["idle_workers"]
            pending_tasks = self.real_time_status[executor]["pending_tasks"]
            launch_queue_task_num = self.real_time_status[executor][
                "launch_queue_task_num"
            ]
            exp_logger.info(
                f"[RealTimeWorkerInfo] after submit {task_str} {executor}|{total_workers}|{idle_workers}|{pending_tasks}|{launch_queue_task_num}"
            )

    def calculate_ideal_execution_duration(self, executor, task_record):
        if self.real_time_status[executor]["total_workers"] == 0:
            return float("inf")
        return (
            self.cur_being_executed_time[executor]
            / self.real_time_status[executor]["total_workers"]
            + task_record["predict_execution"][executor]
        )

    def set_pre_scale_flag(self, executor):
        self.real_time_status[executor]["pre_scale"] = True

    def modify_launch_queue_task_num(self, executor, num):
        self.real_time_status[executor]["launch_queue_task_num"] += num

    def update_from_result_handler(self, result, executor, task_record):
        """
        Update the resource status from result handler
        """
        # change tmp_status then the original dict will be also changed
        # history code, using real_time_status will be more accurate
        # result["total_workers"] = self.not_changed_total_workers[executor]
        import time

        if "predict_execution" in task_record.keys():
            self.cur_being_executed_time[executor] -= task_record["predict_execution"][
                executor
            ]

        tmp_status = self.resource_status[executor]
        tmp_status["active_managers"] = result["active_managers"]
        tmp_status["idle_workers"] = result["idle_workers"]
        tmp_status["total_workers"] = result["total_workers"]
        tmp_status["pending_tasks"] = result["pending_tasks"]
        if (
            "cpu_freqs_max" in result.keys()
            and "cpu_freqs_min" in result.keys()
            and "cpu_freqs_current" in result.keys()
        ):
            cpu_max = max(
                result["cpu_freqs_max"],
                result["cpu_freqs_min"],
                result["cpu_freqs_current"],
            )
            result["cpu_freqs_max"] = cpu_max
        tmp_status["cpu_freq"] = result["cpu_freqs_max"]
        # with self.real_time_status[executor]['value_lock']:
        if self.real_time_status[executor]["closed"]:
            self.real_time_status[executor]["closed"] = False
            self.real_time_status[executor]["total_workers"] = result["total_workers"]
            self.real_time_status[executor]["pending_tasks"] = 0
            self.real_time_status[executor]["idle_workers"] = result["total_workers"]
            exp_logger.info(
                f"[RealTimeWorkerInfo] open by result {executor}|{self.real_time_status[executor]['total_workers']}|{self.real_time_status[executor]['idle_workers']}|{self.real_time_status[executor]['pending_tasks']}"
            )
            self.real_time_status[executor]["submit_counter"] = max(
                0, self.real_time_status[executor]["submit_counter"] - 1
            )  # since one task is completed
            self.real_time_status[executor]["total_workers"] = result["total_workers"]
            self.real_time_status[executor]["idle_workers"] = max(
                0,
                self.real_time_status[executor]["total_workers"]
                - self.real_time_status[executor]["submit_counter"],
            )
            self.real_time_status[executor]["pending_tasks"] = max(
                0,
                self.real_time_status[executor]["submit_counter"]
                - result["total_workers"],
            )
            self.real_time_status[executor]["submit_counter"] = 0
            exp_logger.info(
                f"[RealTimeWorkerInfo] handle counter{executor}|{self.real_time_status[executor]['total_workers']}|{self.real_time_status[executor]['idle_workers']}|{self.real_time_status[executor]['pending_tasks']}"
            )
        else:
            if (
                result["total_workers"]
                > self.real_time_status[executor]["total_workers"]
            ):
                exp_logger.info(
                    f"[RealTimeWorkerInfo] update_when_scaling_out result worker {result['total_workers']}, total worker {self.real_time_status[executor]['total_workers']}"
                )
                self.update_when_scaling_out(executor, result["total_workers"])
            elif (
                result["total_workers"]
                < self.real_time_status[executor]["total_workers"]
            ):
                exp_logger.info(
                    f"[RealTimeWorkerInfo] update_when_scaling_in result worker {result['total_workers'] }, total worker {self.real_time_status[executor]['total_workers']}"
                )
                self.update_when_scaling_in(executor, result["total_workers"])

            if self.real_time_status[executor]["pending_tasks"] > 0:
                self.real_time_status[executor]["pending_tasks"] -= 1
            else:
                if (
                    self.real_time_status[executor]["idle_workers"] + 1
                    <= self.real_time_status[executor]["total_workers"]
                ):
                    self.real_time_status[executor]["idle_workers"] += 1
            exp_logger.info(
                f"[RealTimeWorkerInfo] update by result {executor}|{self.real_time_status[executor]['total_workers']}|{self.real_time_status[executor]['idle_workers']}|{self.real_time_status[executor]['pending_tasks']}|{self.real_time_status[executor]['launch_queue_task_num']}"
            )

    def update_when_scaling_out(self, executor, cur_workers):
        original_workers = self.real_time_status[executor]["total_workers"]
        original_idle_workers = self.real_time_status[executor]["idle_workers"]
        original_pending_tasks = self.real_time_status[executor]["pending_tasks"]
        increase_workers = cur_workers - original_workers
        cur_idle_workers = max(
            0,
            cur_workers
            - (original_pending_tasks + (original_workers - original_idle_workers)),
        )
        cur_pending_tasks = max(
            0,
            original_pending_tasks
            + (original_workers - original_idle_workers)
            - (cur_workers - cur_idle_workers),
        )
        self.real_time_status[executor]["total_workers"] = cur_workers
        self.real_time_status[executor]["idle_workers"] = cur_idle_workers
        self.real_time_status[executor]["pending_tasks"] = cur_pending_tasks

    def pure_scale_in(self, executor, scale_in_num):
        for funcx_executor in self.executors:
            if isinstance(funcx_executor, FuncXExecutor):
                if funcx_executor.label == executor:
                    funcx_executor.online_scale_in(scale_in_num)
                    exp_logger.info(f"success pure decrease_worker {scale_in_num}")

    def update_when_scaling_in(self, executor, cur_workers):
        original_workers = self.real_time_status[executor]["total_workers"]
        original_idle_workers = self.real_time_status[executor]["idle_workers"]
        original_pending_tasks = self.real_time_status[executor]["pending_tasks"]
        decrease_workers = original_workers - cur_workers
        for funcx_executor in self.executors:
            if isinstance(funcx_executor, FuncXExecutor):
                if funcx_executor.label == executor:
                    funcx_executor.online_scale_in(decrease_workers)
                    exp_logger.info(f"success decrease_worker{decrease_workers}")

        cur_idle_workers = max(0, original_idle_workers - decrease_workers)

        # cur_pending_tasks = max(0, original_pending_tasks - (original_workers - original_idle_workers) + (original_workers - cur_workers))
        self.real_time_status[executor]["total_workers"] = cur_workers
        self.real_time_status[executor]["idle_workers"] = cur_idle_workers
        if cur_idle_workers == 0:
            self.real_time_status[executor]["pending_tasks"] += max(
                0, decrease_workers - original_idle_workers
            )

    def finish_n_tasks(self, info, executor, n):
        """
        ****Deprecated****
        Finish n tasks, since the mock endpoint could be closed.
        The mock endpoint will be open when result returned.
        This function should be used when the throughput is high.
        The results shoubd be handled immediately.
        """
        if n == 0 or not info:  # zero nums or info empty
            return

        # history code, using real_time_status will be more accurate
        tmp_status = self.resource_status[executor]
        tmp_status["active_managers"] = info["active_managers"]
        tmp_status["idle_workers"] = info["idle_workers"]
        tmp_status["total_workers"] = info["total_workers"]
        tmp_status["pending_tasks"] = info["pending_tasks"]
        if (
            "cpu_freqs_max" in info.keys()
            and "cpu_freqs_min" in info.keys()
            and "cpu_freqs_current" in info.keys()
        ):
            cpu_max = max(
                info["cpu_freqs_max"], info["cpu_freqs_min"], info["cpu_freqs_current"]
            )
            info["cpu_freqs_max"] = cpu_max
        tmp_status["cpu_freq"] = info["cpu_freqs_max"]
        if self.real_time_status[executor]["closed"]:
            self.real_time_status[executor]["closed"] = False
            self.real_time_status[executor]["total_workers"] = info["total_workers"]
            self.real_time_status[executor]["pending_tasks"] = 0
            self.real_time_status[executor]["idle_workers"] = info["total_workers"]
            exp_logger.info(
                f"[RealTimeWorkerInfo] {executor}|{self.real_time_status[executor]['total_workers']}|{self.real_time_status[executor]['idle_workers']}|{self.real_time_status[executor]['pending_tasks']}"
            )
            self.real_time_status[executor]["submit_counter"] = max(
                0, self.real_time_status[executor]["submit_counter"] - n
            )  # since one task is completed
            self.real_time_status[executor]["total_workers"] = info["total_workers"]
            self.real_time_status[executor]["idle_workers"] = max(
                0,
                self.real_time_status[executor]["total_workers"]
                - self.real_time_status[executor]["submit_counter"],
            )
            self.real_time_status[executor]["pending_tasks"] = max(
                0,
                self.real_time_status[executor]["submit_counter"]
                - info["total_workers"],
            )
            self.real_time_status[executor]["submit_counter"] = 0
        else:
            if n > self.real_time_status[executor]["pending_tasks"]:
                original_pending_tasks = self.real_time_status[executor][
                    "pending_tasks"
                ]
                self.real_time_status[executor]["pending_tasks"] = 0
                self.real_time_status[executor]["idle_workers"] += (
                    n - original_pending_tasks
                )
            else:
                self.real_time_status[executor]["pending_tasks"] -= n

            exp_logger.info(
                f"[RealTimeWorkerInfo] {executor}|{self.real_time_status[executor]['total_workers']}|{self.real_time_status[executor]['idle_workers']}|{self.real_time_status[executor]['pending_tasks']}"
            )

    def get_resource_status_by_label(self, executor_label):
        return self.resource_status[executor_label]

    def _cal_ideal_upper_bound_of_workers(self):
        for key in self.resource_status.keys():
            cores_per_unit = min(
                self.resource_status[key]["max_workers_per_node"],
                self.resource_status[key]["cores_per_unit"],
            )
            self.resource_status[key]["max_cores"] = (
                cores_per_unit
                * self.resource_status[key]["max_blocks"]
                * self.resource_status[key]["nodes_per_block"]
            )

    def monitor_resource(self, kill_event, interval=10):
        while not kill_event.is_set():
            for executor in self.resource_status.keys():
                exp_logger.info(
                    f"[RealTimeWorkerInfo] {executor}|{self.real_time_status[executor]['total_workers']}|{self.real_time_status[executor]['idle_workers']}|{self.real_time_status[executor]['pending_tasks']}"
                )
            time.sleep(interval)

    def get_real_time_status(self):
        return self.real_time_status

    def get_resource_status(self):
        return self.resource_status
