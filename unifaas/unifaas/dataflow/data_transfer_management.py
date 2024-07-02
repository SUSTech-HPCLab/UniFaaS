import os
from queue import Queue
from unifaas.dataflow.helper.data_transfer_client import RsyncTransferClient
from unifaas.dataflow.helper.data_transfer_client import SFTPClient
from unifaas.dataflow.helper.data_transfer_client import GlobusTransferClient
from unifaas.dataflow.helper.transfer_predictor import TransferPredictor
from concurrent.futures import Future
from funcx.sdk.file import RemoteFile, RemoteDirectory
from unifaas.dataflow.states import States
import logging
import threading
import time
import pathlib

logger = logging.getLogger("unifaas")
import time

CLIENT_ID = "a0cb81f7-757e-4564-ac93-7fef03368d53"
TOKEN_LOC = os.path.expanduser("~/.funcx/credentials/funcx_sdk_tokens.json")
exp_logger = logging.getLogger("experiment")
UNIFAAS_HOME = os.path.join(pathlib.Path.home(), ".unifaas")


class DataTransferManager(object):
    __instance = None
    __first_init = False

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = object.__new__(cls)
        return cls.__instance

    def __init__(
        self, executors, transfer_type="rsync", password_file=None, bandwith_info=None
    ):
        if DataTransferManager.__first_init is False:
            self.entry_task_queue = Queue()
            self.executors = executors
            self.dummy_endpoint_info_dir = os.path.join(
                UNIFAAS_HOME, "dummy_endpoint_info"
            )
            self.label_to_executor = {}
            self.executor_to_globus = {}
            self.executor_to_data_path = {}
            if transfer_type == "rsync":
                self.dtc = RsyncTransferClient(
                    self.executors, password_file=password_file
                )
            elif transfer_type == "sftp":
                self.dtc = SFTPClient(self.executors)
            else:
                self.dtc = GlobusTransferClient(self.executors)
            self.data_transfer_predictor = TransferPredictor(
                self.executors, bandwith_info=bandwith_info, dtc=self.dtc
            )

            self.not_scheduling_task_queue = Queue()
            self.raw_graph = {}
            self.fu_to_task = {}
            self.retry_interval = 2
            self._check_interval = 2
            self.retry_upper_bound = 3
            self.track_interval = 1
            self.app_dep = {}
            self._kill_event = threading.Event()
            self.active_transfers = {}
            self.group_trans_to_record = {}  # transfer id to task_record

            self._track_transfer_thread = threading.Thread(
                target=self._track_transfer_status,
                args=(self._kill_event,),
                name="Data-Manage-Thread",
            )
            self._track_transfer_thread.daemon = True
            self._track_transfer_thread.start()

            self._handle_not_scheduling_thread = threading.Thread(
                target=self._handle_not_scheduling_task,
                args=(self._kill_event,),
                name="Handlenot-scheduling-Thread",
            )
            self._handle_not_scheduling_thread.daemon = True
            self._handle_not_scheduling_thread.start()
            self.trans_count_in_param = {}

            DataTransferManager.__first_init = True

    def _analyze_data_to_transfer(self, task_record):
        args = task_record["args"]
        kwargs = task_record["kwargs"]
        # list concat
        data_to_trans = DataTransferManager.check_pure_data(
            args
        ) + DataTransferManager.check_pure_data(kwargs)
        return data_to_trans

    def _set_pending_for_group_transfer(self, task_record):
        count = 0
        for dep in task_record["depends"]:
            if isinstance(dep, Future):
                if not dep.done():
                    count += 1
        if count > 0:
            return

        if (
            "finish_group_transfer" in task_record.keys()
            and task_record["finish_group_transfer"]
        ):
            task_record["status"] = States.pending
            exp_logger.debug(f"[latency test]  task {task_record['id']} is in pending")

    def group_transfer(self, task_record):
        if task_record["status"] != States.data_managing:
            return

        new_args, kwargs = self.sanitize_and_wrap(
            task_record["args"], task_record["kwargs"]
        )
        data_to_trans = DataTransferManager.check_data_transfer(
            new_args
        ) + DataTransferManager.check_data_transfer(kwargs)
        task_record["finish_group_transfer"] = False
        # can be replaced by set pending directly
        if len(data_to_trans) == 0:
            task_record["finish_group_transfer"] = True
            self._set_pending_for_group_transfer(task_record)

        task_record["data_trans_times"] += len(data_to_trans)

        with task_record["task_data_trans_lock"]:
            for remote_file in data_to_trans:
                trans_info = self.dtc.transfer(remote_file, task_record["executor"])
                # if the transfer is local, we don't need to track it, just decrease
                if trans_info["status"] == "LOCAL":
                    trans_info["prediction_time"] = 0
                    task_record["data_trans_times"] -= 1
                    logger.info(
                        f"[CaseStudy] Transfer for Task {task_record['id']} with file {trans_info['dest_path']}"
                    )
                    if task_record["data_trans_times"] == 0:
                        task_record["finish_group_transfer"] = True
                        self._set_pending_for_group_transfer(task_record)
                    continue
                else:
                    trans_info[
                        "prediction_time"
                    ] = self.data_transfer_predictor.real_time_predict_for_transfer(
                        0, trans_info["src_host"], trans_info["dest_host"]
                    )
                    if trans_info["task_id"] not in self.group_trans_to_record:
                        self.group_trans_to_record[trans_info["task_id"]] = []
                    self.group_trans_to_record[trans_info["task_id"]].append(
                        task_record
                    )

    def sanitize_and_wrap(self, args, kwargs):
        # Replace item in args
        new_args = []
        try:
            for dep in args:
                if isinstance(dep, Future):
                    new_args.extend([dep.result()])
                else:
                    new_args.extend([dep])

            # Check for explicit kwargs ex, fu_1=<fut>
            for key in kwargs:
                dep = kwargs[key]
                if isinstance(dep, Future):
                    kwargs[key] = dep.result()
        except Exception as e:
            return [], {}

        return new_args, kwargs

    def handle_task_record_data_managing(
        self, task_record, input_data=None, retry=False, invoke=False
    ):
        if task_record["status"] == States.pending:
            return

        exp_logger.debug(
            f"[MicroExp] start data transfer management, task {task_record['id']} at time {time.time()}"
        )

        # if the task_record is the first time to be handled, count the remote_file in param
        if task_record["id"] not in self.trans_count_in_param.keys():
            data_to_trans_in_param = self._analyze_data_to_transfer(task_record)
            with task_record["task_data_trans_lock"]:
                task_record["data_trans_times"] += len(data_to_trans_in_param)
                logger.debug(
                    f"Task {task_record['id']} data_trans_times plus {len(data_to_trans_in_param)} by param, now {task_record['data_trans_times']}"
                )
                exp_logger.debug(
                    f"Task {task_record['id']} data_trans_times plus {len(data_to_trans_in_param)} by param, now {task_record['data_trans_times']}"
                )
            self.trans_count_in_param[task_record["id"]] = data_to_trans_in_param

        if (
            not task_record["status"] == States.data_managing
            and not task_record["status"] == States.dynamic_adjust
        ):
            self.not_scheduling_task_queue.put((task_record, input_data))
            return

        # then count the remote_file in input_data
        if invoke:
            task_record["handle_managing_times"] += 1
            with task_record["task_data_trans_lock"]:
                if input_data is not None:
                    data_to_trans = DataTransferManager.check_data_transfer(input_data)
                    task_record["data_trans_times"] += len(data_to_trans)
                    logger.debug(
                        f"Task {task_record['id']} data_trans_times plus {len(data_to_trans)} by input_data, now {task_record['data_trans_times']}"
                    )
                    exp_logger.debug(
                        f"Task {task_record['id']} data_trans_times plus {len(data_to_trans)} by input_data, now {task_record['data_trans_times']}"
                    )

        with task_record["task_data_trans_lock"]:
            data_to_trans = []
            if task_record["id"] in self.trans_count_in_param.keys():
                data_to_trans += self.trans_count_in_param[task_record["id"]]
            if input_data is not None:
                data_to_trans = data_to_trans + DataTransferManager.check_data_transfer(
                    input_data
                )

            if len(data_to_trans) == 0:
                self._set_pending_if_ready(task_record)
                return

            for remote_file in data_to_trans:
                trans_info = self.dtc.transfer(remote_file, task_record["executor"])
                # if the transfer is local, we don't need to track it, just decrease
                if trans_info["status"] == "LOCAL":
                    exp_logger.debug(
                        f"Transfer local sucess Task{task_record['id']}|{trans_info['task_id']}|{trans_info['src_ep']}|{trans_info['dst_ep']}|{task_record['executor']}"
                    )
                    logger.info(
                        f"[CaseStudy] Transfer for Task {task_record['id']} with file {trans_info['dest_path']}"
                    )
                    task_record["data_trans_times"] -= 1
                    logger.debug(
                        f"Task {task_record['id']} data_trans_times reduce 1 by local, now {task_record['data_trans_times']}"
                    )
                    exp_logger.debug(
                        f"Task {task_record['id']} data_trans_times reduce 1 by local, now {task_record['data_trans_times']}"
                    )
                    self._set_pending_if_ready(task_record)
                    continue
                else:
                    exp_logger.debug(
                        f"Task {task_record['id']}  submit data_trans with Task{trans_info['task_id']}"
                    )
                    self.active_transfers[trans_info["task_id"]] = task_record
            if task_record["id"] in self.trans_count_in_param.keys():
                # clear the data_trans in param
                self.trans_count_in_param[task_record["id"]] = []

    def _set_pending_if_ready(self, task_record):
        # check if all the data dependencies are ready
        # if ready, set the task status to pending
        dep_count = DataTransferManager._count_dep_by_param(
            task_record["args"]
        ) + DataTransferManager._count_dep_by_param(task_record["kwargs"])
        logger.debug(
            f"_set_pending_if_ready Task {task_record['id']} dep_count {dep_count} data_trans_times {task_record['data_trans_times']}"
        )
        exp_logger.debug(
            f"_set_pending_if_ready Task {task_record['id']} dep_count {dep_count} data_trans_times {task_record['data_trans_times']}"
        )
        # handle_managing_times is used to avoid the task is set to pending before the transfer is submitted
        if (
            task_record["data_trans_times"] == 0
            and dep_count == 0
            and task_record["handle_managing_times"] >= len(task_record["depends"])
        ):
            task_record["status"] = States.pending
            exp_logger.debug(
                f"[MicroExp] Task {task_record['id']} set to pending at {time.time()}"
            )
            if task_record["id"] in self.trans_count_in_param.keys():
                self.trans_count_in_param.pop(task_record["id"])
        if (
            "confirm_deps_done" in task_record.keys()
            and task_record["confirm_deps_done"]
        ):
            if task_record["data_trans_times"] == 0 and dep_count == 0:
                task_record["status"] = States.pending
                exp_logger.debug(
                    f"[MicroExp] Task {task_record['id']} set to pending at {time.time()}"
                )
                if task_record["id"] in self.trans_count_in_param.keys():
                    self.trans_count_in_param.pop(task_record["id"])

    def _track_transfer_status(self, kill_event):
        while not kill_event.is_set():
            qsize = self.dtc.transfer_tasks.qsize()
            for i in range(qsize):
                info = self.dtc.transfer_tasks.get()
                if info["status"] == "SUCCEEDED":
                    task_id = info["task_id"]
                    # in group task
                    if task_id in self.group_trans_to_record.keys():
                        for task_record in self.group_trans_to_record[task_id]:
                            with task_record["task_data_trans_lock"]:
                                task_record["data_trans_times"] -= 1
                            if task_record["data_trans_times"] == 0:
                                task_record["finish_group_transfer"] = True
                                self._set_pending_for_group_transfer(task_record)

                            if "speed" in info.keys():
                                exp_logger.info(
                                    f"[CaseStudy] Transfer sucess Task {task_record['id']}|{info['task_id']}|{info['src_ep']}|{info['dst_ep']}|{info['speed']}|{info['transfer_size']}"
                                )
                            logger.info(
                                f"[CaseStudy] Transfer for Task {task_record['id']} with file {info['dest_path']}"
                            )
                        self.group_trans_to_record.pop(task_id)
                        continue
                    if task_id not in self.active_transfers.keys():
                        # in case the task were pushed to transfer_tasks but not active_transfers queue
                        self.dtc.transfer_tasks.put(
                            info
                        )  # this thread is ahead of the thread that push the task to active_transfers
                        continue
                    task_record = self.active_transfers[task_id]
                    if "speed" in info.keys():
                        exp_logger.info(
                            f"Transfer sucess Task{task_record['id']}|{info['task_id']}|{info['src_ep']}|{info['dst_ep']}|{info['speed']}|{info['transfer_size']}"
                        )
                    with task_record["task_data_trans_lock"]:
                        task_record["data_trans_times"] -= 1
                    self._set_pending_if_ready(task_record)
                    if task_id in self.active_transfers.keys():
                        exp_logger.info(
                            f"Transfe task_id {task_id} was poped from active_transfers"
                        )
                        self.active_transfers.pop(task_id)
                elif info["status"] == "FAILED":
                    logger.info("Transfer task {} failed".format(info))
                elif info["status"] == "ACTIVE" or info["status"] == "RETRYING":
                    if info["status"] == "RETRYING":
                        logger.info("Transfer task {} retrying".format(info))
                    self.dtc.transfer_tasks.put(info)
            import time

            time.sleep(self.track_interval)

    @staticmethod
    def _count_dep_fu_without_transfer(task_record):
        dep_count = 0
        for dep in task_record["depends"]:
            if dep.done():
                result = dep.result()
                if not isinstance(result, RemoteFile) and not isinstance(
                    result, RemoteDirectory
                ):
                    dep_count += 1
        return dep_count

    def put_data_management_task(self, task):
        if len(task["depends"]) <= 0:
            self.entry_task_queue.put(task)
        app_fu = task["app_fu"]
        self.fu_to_task[app_fu] = task
        self._generate_raw_graph(task)

    @staticmethod
    def check_data_transfer(res):
        try:
            from concurrent.futures import Future

            if isinstance(res, Future) and res.done():
                res = res.result()
            data_trans_list = []
            if isinstance(res, RemoteFile) or isinstance(res, RemoteDirectory):
                data_trans_list.append(res)

            if isinstance(res, list) or isinstance(res, set) or isinstance(res, tuple):
                for item in res:
                    data_trans_list += DataTransferManager.check_data_transfer(item)

            if isinstance(res, dict):
                for val in res.values():
                    data_trans_list += DataTransferManager.check_data_transfer(val)
        except Exception as e:
            return []

        return data_trans_list
    

    @staticmethod
    def check_pure_data(res):
        # do not detect the future type data in this function
        try:

            data_trans_list = []
            if isinstance(res, RemoteFile) or isinstance(res, RemoteDirectory):
                data_trans_list.append(res)

            if isinstance(res, list) or isinstance(res, set) or isinstance(res, tuple):
                for item in res:
                    data_trans_list += DataTransferManager.check_pure_data(item)

            if isinstance(res, dict):
                for val in res.values():
                    data_trans_list += DataTransferManager.check_pure_data(val)
        except Exception as e:
            return []

        return data_trans_list

    @staticmethod
    def _count_dep_by_param(res):
        dep_count = 0
        try:
            from concurrent.futures import Future

            if isinstance(res, Future) and not res.done():
                dep_count += 1

            if isinstance(res, tuple):
                for item in res:
                    if isinstance(item, Future) and not item.done():
                        dep_count += 1

            if isinstance(res, list):
                for item in res:
                    if isinstance(item, Future) and not item.done():
                        dep_count += 1
            if isinstance(res, dict):
                for val in res.values():
                    if isinstance(val, Future) and not val.done():
                        dep_count += 1
        except Exception as e:
            pass
        return dep_count

    def _generate_raw_graph(self, task):
        if self.raw_graph.get(task["app_fu"]) is None:
            self.raw_graph[task["app_fu"]] = []
        for dep in task["depends"]:
            self.raw_graph[dep].append(task["app_fu"])
        return

    def get_child_task(self, task_record):
        appfu = task_record["app_fu"]
        child_task_fu_list = self.raw_graph[appfu]
        child_task_list = [self.fu_to_task[fu] for fu in child_task_fu_list]
        return child_task_list

    def _handle_not_scheduling_task(self, kill_event):
        """
        There is a special case that the task is not scheduling, but the transfer is invoked by its predecessor.
        At this time, the transfer will be skipped until the task is scheduled.
        """
        while not kill_event.is_set():
            if self.not_scheduling_task_queue.qsize() > 0:
                cur_size = self.not_scheduling_task_queue.qsize()
                for i in range(cur_size):
                    (task_record, input_data) = self.not_scheduling_task_queue.get()
                    if (
                        task_record["status"] == States.data_managing
                        or task_record["status"] == States.dynamic_adjust
                    ):
                        self.handle_task_record_data_managing(
                            task_record, input_data, retry=True, invoke=True
                        )
                    else:
                        self.not_scheduling_task_queue.put((task_record, input_data))
                        continue
            time.sleep(0.1)
