import random
import threading
from unifaas.dataflow.states import States
from unifaas.executors.funcx.executor import FuncXExecutor
from queue import PriorityQueue
from queue import Queue
from enum import Enum
import time
from unifaas.dataflow.helper.graph_helper import graphHelper
import logging
from concurrent.futures import Future
from unifaas.dataflow.helper.execution_predictor import ExecutionPredictor
from unifaas.dataflow.helper.transfer_predictor import TransferPredictor
from unifaas.dataflow.helper.resource_status_poller import ResourceStatusPoller
from unifaas.dataflow.data_transfer_management import DataTransferManager
from unifaas.dataflow.schedule_strategy.heft import heft_schedule_entry
from unifaas.dataflow.schedule_strategy.ic_optimal import ic_optimal_scheduling
from unifaas.dataflow.schedule_strategy.dynamic_heft import dynamic_heft_scheduling
from unifaas.dataflow.schedule_strategy.auto import AutoScheduling
from unifaas.dataflow.schedule_strategy.ep_selection import GreedySelection
from unifaas.dataflow.futures import AppFuture

SCHEDULING_STRATEGY = ["RANDOM", "DFS", "DATA", "FIFO", "DHEFT"]
DEPRECATED_STRATEGY = ["ADVANCE", "HEFT", "AUTO", "IC_Opt"]
exp_logger = logging.getLogger("experiment")


class Scheduler:
    def __init__(
        self,
        executors,
        enable_execution_predictor=True,
        scheduling_strategy=None,
        recorder=None,
        workflow_name="default",
        duplicated_tasks=None,
        enable_duplicate=False,
    ):
        if scheduling_strategy is None:
            self.scheduling_strategy = "RANDOM"
        elif scheduling_strategy not in SCHEDULING_STRATEGY:
            raise Exception("Invalid scheduling strategy")
        else:
            self.scheduling_strategy = scheduling_strategy
        if enable_execution_predictor and recorder is not None:
            self.execution_predictor = ExecutionPredictor(
                executors, recorder, workflow_name
            )
            self.transfer_predictor = TransferPredictor(executors)
        self.executors = executors
        self.FIFO_ready_queue = Queue()

        self.eft_queue = {}
        self.resource_poller = ResourceStatusPoller(self.executors)
        self.resources = self.resource_poller.get_resource_status()
        self.duplicated_tasks = duplicated_tasks
        self.data_manager = DataTransferManager(self.executors)
        if self.scheduling_strategy == "AUTO":
            self.auto_scheduling = AutoScheduling(
                self.resource_poller,
                self.execution_predictor,
                self.transfer_predictor,
                self.data_manager,
            )

        exp_logger.info(
            "[CaseStudy] Scheduling strategy: {}".format(self.scheduling_strategy)
        )

        self._kill_event = threading.Event()
        self._task_schedule_thread = threading.Thread(
            target=self.schedule, args=(self._kill_event,), name="Task-Schedule-Thread"
        )
        self._task_schedule_thread.daemon = True
        self._task_schedule_thread.start()

        self.base_count_for_duplicated_task = 10**8
        self.duplicated_task_count = self.base_count_for_duplicated_task

        self.task_duplicate_queue = Queue()
        self.enable_duplicate = enable_duplicate
        self._task_duplicate_thread = threading.Thread(
            target=self.duplicate_schedule, name="Task-Duplicate-Thread"
        )
        self._task_duplicate_thread.daemon = True
        self._task_duplicate_thread.start()

        self.ep_to_label = {}
        self._init_ep_to_label()
        self.scheduling_tasks_que = Queue()
        self.scheduling_tasks_que_copy = Queue()
        self.raw_graph = {}
        self.fu_to_task = {}  # key: app_fu value: task
        self.dynamic_adjust_strategy = None

        self.data_ready_queue_dict = {}
        for ep in self.resources.keys():
            self.data_ready_queue_dict[ep] = PriorityQueue()

        self.data_compress_task_que = Queue()
        self.data_compress_target_task_que = Queue()
        if self.scheduling_strategy == "IC_Opt":
            self.ep_selection = GreedySelection(
                self.resource_poller,
                self.execution_predictor,
                self.transfer_predictor,
                self.data_manager,
                "IC",
            )
        if self.scheduling_strategy == "DHEFT":
            self.ep_selection = GreedySelection(
                self.resource_poller,
                self.execution_predictor,
                self.transfer_predictor,
                self.data_manager,
                "HEFT",
            )

    def put_important_task_into_duplicated_queue(self, task_record):
        self.task_duplicate_queue.put(task_record)

    def duplicate_schedule(self):
        while not self._kill_event.is_set():
            cur_qsize = self.task_duplicate_queue.qsize()
            counter = cur_qsize
            while counter > 0:
                counter -= 1
                task_record = self.task_duplicate_queue.get()
                if not self.enable_duplicate:
                    continue
                if "original_task" in task_record.keys():
                    continue
                if "predict_execution" not in task_record.keys():
                    continue
                if task_record["status"] == States.scheduling:
                    self.task_duplicate_queue.put(task_record)

                if (
                    task_record["status"] != States.pending
                    and task_record["status"] != States.running
                    and task_record["status"] != States.launched
                ):
                    continue

                # check if important, duplicate important task
                if (
                    task_record["important"]
                    and "duplicated_at" not in task_record.keys()
                ):
                    min_executor = None
                    min_time = float("inf")
                    if "predict_execution" in task_record.keys():
                        for key in task_record["predict_execution"]:
                            if task_record["predict_execution"][key] < min_time:
                                min_time = task_record["predict_execution"][key]
                                min_executor = key
                        if min_executor != task_record["executor"]:
                            ideal_increse = (
                                task_record["predict_execution"][
                                    task_record["executor"]
                                ]
                                - task_record["predict_execution"][min_executor]
                            )
                            exp_logger.info(
                                "[Duplicate] Try to duplicate important task {} from {} to {} with ideal increase {}s".format(
                                    task_record["id"],
                                    task_record["executor"],
                                    min_executor,
                                    ideal_increse,
                                )
                            )
                            task_record["duplicated_at"] = min_executor
                            duplicated_task = self.copy_duplicated_task(task_record)
                            self.submit_duplicated_task(duplicated_task)
                            self.duplicated_tasks[
                                duplicated_task["id"]
                            ] = duplicated_task
                        else:
                            exp_logger.info(
                                f"[Duplicate] Important task {task_record['id']} is already assigned to the best executor."
                            )

                    # procedure for important task should be over
                    continue

                cur_executor = task_record["executor"]
                cur_resource_status = self.resource_poller.get_real_time_status()
                min_executor = None
                if (
                    cur_resource_status[cur_executor]["idle_workers"] == 0
                    and cur_resource_status[cur_executor]["pending_tasks"]
                    > cur_resource_status[cur_executor]["total_workers"]
                ):
                    other_executor = []
                    for key in cur_resource_status.keys():
                        if key != cur_executor:
                            other_executor.append(key)
                    for executor in other_executor:
                        if cur_resource_status[executor]["total_workers"] == 0:
                            continue
                        if (
                            cur_resource_status[executor]["idle_workers"]
                            / cur_resource_status[executor]["total_workers"]
                            > 0.6
                        ):
                            min_executor = executor
                            break
                if min_executor is not None:
                    exp_logger.info(
                        "[Duplicate] Duplicate task {} from {} to {}".format(
                            task_record["id"], task_record["executor"], min_executor
                        )
                    )
                    task_record["duplicated_at"] = min_executor
                    duplicated_task = self.copy_duplicated_task(task_record)
                    self.submit_duplicated_task(duplicated_task)
                    self.duplicated_tasks[duplicated_task["id"]] = duplicated_task
                    continue

                # duplicate scheduling
                # 1. Choose the best executor
                min_executor = None
                min_time = float("inf")
                if "predict_execution" not in task_record.keys():
                    continue

                for key in task_record["predict_execution"]:
                    if task_record["predict_execution"][key] < min_time:
                        min_time = task_record["predict_execution"][key]
                        min_executor = key

                if min_executor is None or min_executor == task_record["executor"]:
                    self.task_duplicate_queue.put(task_record)
                    continue

                # 2. Check if the delta time is larger than transfer time
                max_transfer_time = 0
                for key1 in task_record["predict_execution"]:
                    for key2 in task_record["predict_execution"]:
                        tmp_cost = self.transfer_predictor.predict_comm_cost(
                            task_record, key1, key2
                        )
                        max_transfer_time = max(max_transfer_time, tmp_cost)

                delta_time = (
                    task_record["predict_execution"][task_record["executor"]] - min_time
                )
                # if delta_time < max_transfer_time or  delta_time < 120:
                #  continue
                if delta_time < 500:
                    self.task_duplicate_queue.put(task_record)
                    continue

                # 3. Check the idle workers of the executor
                cur_resource = self.resource_poller.get_real_time_status()
                cur_idle_workers = cur_resource[min_executor]["idle_workers"]
                cur_total_workers = cur_resource[min_executor]["total_workers"]
                if (
                    cur_idle_workers * 0.5 < cur_qsize
                    or cur_idle_workers < cur_total_workers * 0.2
                ):
                    self.task_duplicate_queue.put(task_record)
                    continue

                exp_logger.info(
                    "[Duplicate] Try to duplicate task {} from {} to {}".format(
                        task_record["id"], task_record["executor"], min_executor
                    )
                )
                task_record["duplicated_at"] = min_executor
                duplicated_task = self.copy_duplicated_task(task_record)
                self.submit_duplicated_task(duplicated_task)
                self.duplicated_tasks[duplicated_task["id"]] = duplicated_task
            time.sleep(0.5)

    def copy_duplicated_task(self, task_record):
        duplicated_task_record = task_record.copy()
        task_id = self.duplicated_task_count
        self.duplicated_task_count += 1
        duplicated_task_record.update(
            {
                "id": task_id,
                "visited": False,
                "modified_times": 0,
                "submitted_to_poller": False,
                "never_change": False,
                "task_launch_lock": threading.Lock(),
                "task_data_trans_lock": threading.Lock(),
                "data_trans_times": 0,
                "handle_managing_times": 0,
            }
        )
        duplicated_task_record["executor"] = task_record["duplicated_at"]
        app_fu = AppFuture(duplicated_task_record)
        duplicated_task_record["app_fu"] = app_fu
        duplicated_task_record["original_task"] = task_record
        duplicated_task_record.pop("duplicated_at")
        return duplicated_task_record

    def put_scheduling_task(self, task):
        self.scheduling_tasks_que.put(task)
        self.scheduling_tasks_que_copy.put(task)
        app_fu = task["app_fu"]
        self.fu_to_task[app_fu] = task
        self.insert_app_fu_vertex(task)

    # Build DAG top-down(inverse the dependency)
    def insert_app_fu_vertex(self, task):
        if self.raw_graph.get(task["app_fu"]) is None:
            self.raw_graph[task["app_fu"]] = []
        for dep in task["depends"]:
            self.raw_graph[dep].append(task["app_fu"])

    def _init_ep_to_label(self):
        for ep in self.executors.values():
            if isinstance(ep, FuncXExecutor):
                self.ep_to_label[ep.endpoint] = ep.label

    def kill_scheduler(self):
        self._kill_event.set()
        self._task_schedule_thread.join()

    def ic_optimal_scheduling_entry(self):
        # Note: This function is deprecated, and not fully implemented.
        # We did not mention this strategy in our paper (UniFaaS).
        # See more information about the IC-Optimal scheduling in this paper
        # Advances in IC-Scheduling Theory: Scheduling Expansive and Reductive DAGs and Scheduling DAGs via Duality
        time.sleep(0.5)
        self.dynamic_adjust_strategy = "GREEDY"
        (
            task_execution_order,
            check_sufficient_info,
            peak_count,
            important_task_list,
        ) = ic_optimal_scheduling(graphHelper, self.execution_predictor)
        for task in important_task_list:
            self.put_important_task_into_duplicated_queue(task)
        if task_execution_order.qsize() > 0:
            exp_logger.info(
                "[IC_Opt] The length of task execution order is {}".format(
                    task_execution_order.qsize()
                )
            )

        self.ep_selection.assign_for_queue(task_execution_order)
        return

    def dynamic_heft_entry(self):
        time.sleep(0.5)
        self.dynamic_adjust_strategy = "GREEDY"
        (
            task_execution_order,
            check_sufficient_info,
            ratio_result,
        ) = dynamic_heft_scheduling(
            graphHelper, self.execution_predictor, self.transfer_predictor
        )
        if task_execution_order.qsize() > 0:
            exp_logger.info(
                "[DynamicHEFT] The length of task execution order is {}".format(
                    task_execution_order.qsize()
                )
            )
            exp_logger.info("[DynamicHEFT] The ratio result is {}".format(ratio_result))
        self.ep_selection.update_performance_ratio(ratio_result)
        self.ep_selection.assign_for_queue(task_execution_order)
        return

    def schedule(self, kill_event):
        while not kill_event.is_set():
            if self.scheduling_strategy == "RANDOM":
                self.random_schedule()
            if self.scheduling_strategy == "DFS":
                self.dfs_based_schedule()
            if self.scheduling_strategy == "DATA":
                self.data_locality_schedule(graphHelper.task_queue)
            if self.scheduling_strategy == "DHEFT":
                self.dynamic_heft_entry()
            if self.scheduling_strategy == "FIFO":
                self.FIFO_schedule()

            # ** These strategies are deprecated and not mentioned in the paper **
            # if self.scheduling_strategy == "ADVANCE":
            #     self._init_eft_queue()
            #     self.advance_schedule()
            # if self.scheduling_strategy == "HEFT":
            #     self.heft_schedule()
            # if self.scheduling_strategy == "AUTO":
            #     strategy = self.auto_scheduling.select_strategy()
            #     if strategy == "HEFT":
            #         self.scheduling_strategy = "HEFT"
            #     elif strategy == "DATA":
            #         self.scheduling_strategy = "DATA"
            #     elif strategy == "PART":
            #         self.auto_scheduling.AUTO_data_locality()
            #         # after handling all the unknown tasks, switch back to HEFT
            #         self.auto_scheduling.AUTO_heft_schedule()
            #         self.scheduling_strategy = "NO_OP"
            # if self.scheduling_strategy == "IC_Opt":
            #     self.ic_optimal_scheduling_entry()
            # if self.scheduling_strategy == "NO_OP":
            #     time.sleep(10)

    def pre_scale_out(self, cur_tasks):
        cur_real_time_status = self.resource_poller.get_real_time_status()
        idle_nums = 0
        to_scale_out = []
        for executor in cur_real_time_status:
            idle_nums += cur_real_time_status[executor]["idle_workers"]
            if cur_real_time_status[executor]["closed"]:
                to_scale_out.append(executor)
        if idle_nums < cur_tasks:
            for executor in to_scale_out:
                self.executors[executor].scale_out(
                    self.resource_poller, cur_real_time_status[executor]["max_blocks"]
                )
        return

    def all_resource_scale_out(self):
        cur_real_time_status = self.resource_poller.get_real_time_status()
        for executor in cur_real_time_status.keys():
            self.executors[executor].scale_out(
                self.resource_poller, cur_real_time_status[executor]["max_blocks"]
            )
        return

    def put_task_to_data_ready_queue(self, task_record):
        for executor in self.resources.keys():
            task_with_transfer_size = TaskWithTransferSize(
                task_record, executor, self.transfer_predictor
            )
            self.data_ready_queue_dict[executor].put(task_with_transfer_size)
        return

    def _DATA_fetch_minimum_transfer_task(self, executor):
        ready_q = self.data_ready_queue_dict[executor]
        while not ready_q.empty():
            task_with_transfer_size = ready_q.get()
            if task_with_transfer_size.task_record["status"] == States.scheduling:
                return task_with_transfer_size.task_record
        return None

    def _DATA_get_data_ready_queue_size(self):
        q_size = float("inf")
        for key in self.data_ready_queue_dict.keys():
            ready_q = self.data_ready_queue_dict[key]
            q_size = min(q_size, ready_q.qsize())
        return q_size
    
    def _DATA_submit_compression_target_task(self,task_record):
        if task_record['compress_option'][3] is None:
            raise RuntimeError("Don't put a non compression target task into the queue")
        self.data_compress_target_task_que.put(task_record)
        for dep in task_record['depends']:
            if not dep.done():
                dep_task = dep.task_def
                if dep_task['compress_option'][2] is not None:
                    dep_task['executor'] = task_record['executor']
                    self.resource_poller.update_status_when_submit_one_task(dep_task['executor'])
                    dep_task["submitted_to_poller"] = True
                    dep_task["status"] = States.data_managing
                    self.data_manager.group_transfer(dep_task)


    def _DATA_handle_ready(self):
        # firstly submit compress task
        while not self.data_compress_task_que.empty():
            task_record = self.data_compress_task_que.get()
            target_ep = task_record['executor']  # executor is assigned before putting into the queue 
            self.resource_poller.update_status_when_submit_one_task(target_ep)
            task_record["submitted_to_poller"] = True
            task_record["status"] = States.data_managing
            self.data_manager.group_transfer(task_record)

        # secondly submit compress target task periodically
        cur_target_size = self.data_compress_target_task_que.qsize()
        while cur_target_size > 0:
            task_record = self.data_compress_target_task_que.get()
            all_done = True
            for dep in task_record['depends']:
                if isinstance(dep, Future) and not dep.done():
                    all_done = False
            if all_done:
                self.resource_poller.update_status_when_submit_one_task(task_record['executor'])
                task_record["submitted_to_poller"] = True
                task_record["status"] = States.data_managing
                self.data_manager.group_transfer(task_record)
            else:
                self.data_compress_target_task_que.put(task_record)
            cur_target_size -= 1


        cur_resource = self.resource_poller.get_real_time_status()
        idle_workers = []
        endpoints = []
        for ep in cur_resource.keys():
            redundant_cap = 0
            idle_workers.append(
                max(
                    0,
                    cur_resource[ep]["idle_workers"]
                    - cur_resource[ep]["launch_queue_task_num"]
                    + redundant_cap,
                )
            )
            endpoints.append(ep)
            q_size = self._DATA_get_data_ready_queue_size()
            if cur_resource[ep]["total_workers"] == 0 and q_size > 0:
                if not cur_resource[ep]["pre_scale"]:
                    self.executors[ep].scale_out(self.resource_poller)
                    self.resource_poller.set_pre_scale_flag(ep)
        attempt_times = sum(idle_workers)
        while attempt_times > 0:
            for i in range(len(idle_workers)):
                target_ep = endpoints[i]
                if idle_workers[i] > 0:
                    task_record = self._DATA_fetch_minimum_transfer_task(target_ep)
                    if task_record is None:
                        continue
                    if not task_record["never_change"]:
                        task_record["executor"] = target_ep
                        
                    if task_record['compress_option'][3] is not None:
                        # do not transfer task until deps finished
                        self._DATA_submit_compression_target_task(task_record)
                        task_record["submitted_to_poller"] = True
                        task_record["status"] = States.data_managing
                        idle_workers[i] -= 1
                        break


                    self.resource_poller.update_status_when_submit_one_task(target_ep)
                    task_record["submitted_to_poller"] = True
                    task_record["status"] = States.data_managing
                    self.data_manager.group_transfer(task_record)
                    idle_workers[i] -= 1
                    break
            attempt_times -= 1

    def _DATA_put_compress_task_to_que(self, task_record):
        if 'compress_option' in task_record and task_record['compress_option'][1] is not None:
            task_record['executor'] = task_record['depends'][0].task_def['executor']
            self.data_compress_task_que.put(task_record)
            return True
        
        return False

    def data_locality_schedule(self, task_queue):
        # TODO: This scheduling method should be moved in to ep_selection file
        time.sleep(0.5)
        cur_qsize = task_queue.qsize()
        # self.all_resource_scale_out()
        while cur_qsize > 0:
            task_record = task_queue.get()
            if task_record["status"] == States.scheduling:
                all_done = True
                for parent in task_record["depends"]:
                    if isinstance(parent, Future) and not parent.done():
                        all_done = False
                        break
                if all_done:
                    succ = self._DATA_put_compress_task_to_que(task_record)
                    if not succ:
                        self.put_task_to_data_ready_queue(task_record)
            cur_qsize -= 1
        self._DATA_handle_ready()

    def _select_endpoint_based_on_size(self, task_record):
        size_stat = {}
        resource = self.resource_poller.get_real_time_status()
        for key in resource.keys():
            size_stat[key] = 0
        for dep in task_record["depends"]:
            executor = dep.task_def["executor"]
            size_stat[executor] += dep.task_def["output_size"]
        return min(size_stat, key=size_stat.get)

    def _select_endpoint_based_on_ratio(self):
        resources = self.resource_poller.get_real_time_status()
        sum_workers = 0
        ratio = []
        for ep in resources.keys():
            sum_workers += resources[ep]["max_cores"]
        for ep in resources.keys():
            ratio.append(resources[ep]["max_cores"] / sum_workers)
        executor = random.choices(population=list(resources.keys()), weights=ratio, k=1)
        executor = executor[0] if isinstance(executor, list) else executor
        return executor

    def _FIFO_handle_ready(self):
        cur_resource = self.resource_poller.get_real_time_status()
        idle_workers_info = {}
        ready_num = self.FIFO_ready_queue.qsize()
        redundant_cap = 0
        for ep in cur_resource.keys():
            # submit -> launch_queue_task -> idle worker/pending tasks
            idle_workers_info[ep] = max(
                0,
                cur_resource[ep]["idle_workers"]
                - cur_resource[ep]["launch_queue_task_num"]
                + redundant_cap,
            )
            if cur_resource[ep]["total_workers"] == 0 and ready_num > 0:
                if not cur_resource[ep]["pre_scale"]:
                    self.executors[ep].scale_out(self.resource_poller)
                    self.resource_poller.set_pre_scale_flag(ep)

        sum_idle_workers = sum(idle_workers_info.values())
        while sum_idle_workers > 0 and ready_num > 0:
            task_record = self.FIFO_ready_queue.get()
            if task_record["status"] == States.scheduling:
                idle_endpoints = []
                for ep in idle_workers_info.keys():
                    if idle_workers_info[ep] > 0:
                        idle_endpoints.append(ep)
                traget_ep = random.choice(idle_endpoints)
                if not task_record["never_change"]:
                    task_record["executor"] = traget_ep
                final_ep = task_record["executor"]
                self.resource_poller.update_status_when_submit_one_task(final_ep)
                task_record["submitted_to_poller"] = True
                task_record["status"] = States.data_managing
                self.data_manager.group_transfer(task_record)
                idle_workers_info[final_ep] -= 1
                ready_num -= 1
                sum_idle_workers -= 1

    def submit_duplicated_task(self, task_record):
        self.resource_poller.update_status_when_submit_one_task(task_record["executor"])
        task_record["submitted_to_poller"] = True
        task_record["status"] = States.data_managing
        self.data_manager.group_transfer(task_record)

    def FIFO_schedule(self):
        """
        Firstly assign all head tasks to the executors.
        When a head task is done, assign the child task in the order of FIFO.
        """
        time.sleep(0.5)  # waiting for the first batch of tasks
        cur_qsize = graphHelper.task_queue.qsize()
        start_time = time.time()
        while cur_qsize > 0:
            task_record = graphHelper.task_queue.get()
            if task_record["status"] == States.scheduling:
                all_done = True
                for parent in task_record["depends"]:
                    if isinstance(parent, Future) and not parent.done():
                        all_done = False
                        break
                if all_done:
                    self.FIFO_ready_queue.put(task_record)
            cur_qsize -= 1

        self._FIFO_handle_ready()

    def heft_schedule(self):
        time.sleep(20)
        fu_to_task = {}
        pure_dag = {}
        while time.time() - graphHelper.get_task_idle_since() < 5:
            time.sleep(1)

        cur_qsize = graphHelper.task_queue.qsize()

        while cur_qsize > 0:
            task_record = graphHelper.task_queue.get()
            app_fu = task_record["app_fu"]
            fu_to_task[app_fu] = task_record
            if app_fu not in pure_dag.keys():
                pure_dag[app_fu] = []
            for dep in task_record["depends"]:
                # make sure pred node is not scheduled.
                if dep.task_def["status"] == States.scheduling:
                    if dep in pure_dag.keys():
                        pure_dag[dep].append(app_fu)
                    else:
                        pure_dag[dep] = [app_fu]
            cur_qsize -= 1
        machines = self.resource_poller.get_real_time_status().keys()
        exp_logger.info(f"Doing heft scheduling")
        time_start = time.time()
        _, allocate_res = heft_schedule_entry(
            pure_dag,
            machines,
            fu_to_task,
            self.execution_predictor,
            self.transfer_predictor,
            self.resource_poller,
        )
        time_end = time.time()
        exp_logger.info(
            f"Heft scheduling time: {time_end - time_start} with {len(allocate_res.keys())} tasks"
        )

        counter = 0
        for fu in allocate_res.keys():
            task_record = fu_to_task[fu]
            not_done_deps = [dep for dep in task_record["depends"] if not dep.done()]
            if len(not_done_deps) <= 0:
                task_record["status"] = States.data_managing
                if not task_record["never_change"]:
                    task_record["executor"] = allocate_res[fu]
                self.data_manager.handle_task_record_data_managing(task_record)
            else:
                task_record["status"] = States.dynamic_adjust
                if not task_record["never_change"]:
                    task_record["executor"] = allocate_res[fu]
            counter += 1


    def assign_same_executor_with_compressed_task(self,task_record):
        if len(task_record["depends"]) == 1 and 'compress_option' in task_record  and task_record["compress_option"][1] is not None:
            pre_task  = task_record["depends"][0].task_def
            task_record["executor"] = pre_task["executor"]
            return

        # check if the depend task is a decompression task
        for dep in task_record["depends"]:
            pre_task = dep.task_def
            if 'compress_option' in pre_task and pre_task["compress_option"][2] is not None:
                pre_task['executor'] = task_record['executor']
            


    def random_schedule(self):
        batch_size = 500
        cur_qsize = graphHelper.task_queue.qsize()
        time_interval = 5
        duration = 0
        exp_logger.debug(
            f"[MicroExp] now time {time.time()} and last ideal time {graphHelper.get_task_idle_since()}"
        )

        if (
            cur_qsize >= batch_size
            or time.time() - graphHelper.get_task_idle_since() > time_interval
        ):
            starting_time = time.time()
            exp_logger.debug(
                f"[MicroExp] start random based scheduling at time {time.time()}"
            )
            cur_resource = self.resource_poller.get_real_time_status()
            executor_list = list(cur_resource.keys())
            cur_qsize = graphHelper.task_queue.qsize()
            while cur_qsize > 0:
                task_record = graphHelper.task_queue.get()
                if task_record["status"] == States.scheduling:
                    if not task_record["never_change"]:
                        task_record["executor"] = random.choice(executor_list)
                        self.assign_same_executor_with_compressed_task(task_record)
                    if len(task_record["depends"]) <= 0:
                        task_record["status"] = States.data_managing
                        self.data_manager.handle_task_record_data_managing(task_record)
                    else:
                        task_record["status"] = States.dynamic_adjust
                    if int(task_record["id"]) % 100 == 1:
                        exp_logger.debug(
                            f"[MicroExp] end scheduling, task {task_record['id']} at time {time.time()}"
                        )
                cur_qsize -= 1
            duration = time.time() - starting_time
        exp_logger.debug(
            f"[MicroExp] end once scheduling at time {time.time()} with duration1 {duration}"
        )
        time.sleep(max(0, time_interval - duration))

    def dfs_based_schedule(self):
        batch_size = 50000
        cur_qsize = graphHelper.task_queue.qsize()
        time_interval = 5
        exp_logger.debug(
            f"[MicroExp] now time {time.time()} and last ideal time {graphHelper.get_task_idle_since()}"
        )
        duration = 0
        if (
            cur_qsize >= batch_size
            or time.time() - graphHelper.get_task_idle_since() > time_interval
        ):
            exp_logger.debug(
                f"[MicroExp] start dfs based scheduling at time {time.time()}"
            )
            starting_time = time.time()
            # rank the resource by the number of ideal max workers
            cur_resource = self.resource_poller.get_resource_status()
            real_time_resource = self.resource_poller.get_real_time_status()
            resource_rank = list(cur_resource.keys())

            def get_ideal_worker_nums(label):
                return real_time_resource[label]["total_workers"]

            resource_rank.sort(key=get_ideal_worker_nums, reverse=True)
            cur_qsize = graphHelper.task_queue.qsize()
            entry_qsize = graphHelper.entry_task_queue.qsize()
            partition_res = self.partition_by_ideal_worker_nums(
                cur_qsize, resource_rank
            )
            if cur_qsize > 0:
                exp_logger.info(f"[DFS] DFS ratio is {partition_res}")
            cur_vis = 1
            while 0 < entry_qsize:
                head_task = graphHelper.entry_task_queue.get()
                head_fu = head_task["app_fu"]
                cur_vis = self.dfs_on_raw_graph(
                    head_fu, cur_vis, resource_rank, partition_res
                )
                entry_qsize -= 1
            while cur_qsize > 0:
                task_record = graphHelper.task_queue.get()
                if task_record.get("visited") is not True:
                    graphHelper.entry_task_queue.put(task_record)
                    graphHelper.task_queue.put(task_record)
                    graphHelper.update_task_idle_since()
                    cur_qsize -= 1
                    continue
                if task_record["status"] == States.scheduling:
                    if len(task_record["depends"]) <= 0:
                        task_record["status"] = States.data_managing
                        self.data_manager.handle_task_record_data_managing(task_record)
                    else:
                        task_record["status"] = States.dynamic_adjust
                cur_qsize -= 1
            duration = time.time() - starting_time
        exp_logger.debug(
            f"[MicroExp] end once scheduling at time {time.time()} with duration {duration}"
        )
        time.sleep(max(0, time_interval - duration))

    def _init_eft_queue(self):
        cur_resource = self.resource_poller.get_resource_status()
        self.ideal_workers = {}
        for key in cur_resource.keys():
            self.eft_queue[key] = PriorityQueue()
            self.ideal_workers[key] = cur_resource[key]["max_cores"]

    # This function is used to advance scheduling (task stealing)
    def _stat_eft_info_before_managing(self, task_record):
        # fill the eft info based on predict result
        target_executor = task_record["executor"]
        if "predict_execution" not in task_record.keys():
            self.execution_predictor.predict(
                task_record
            )  # predict_execution, predict_output

        est = self.transfer_predictor.predict_est_for_certain_executor(
            task_record, target_executor
        )
        cur_time = time.time()
        if est == 0:
            est = cur_time
        dispatch_latency = 3

        # estimate the
        cur_size = self.eft_queue[target_executor].qsize()
        execution_time = task_record["predict_execution"][target_executor]
        if cur_size < self.ideal_workers[target_executor]:
            self.eft_queue[target_executor].put(est + dispatch_latency + execution_time)
            task_record["predict_eft"] = est + dispatch_latency + execution_time
        elif cur_size == self.ideal_workers[target_executor]:
            earliest_ft = self.eft_queue[target_executor].get()
            self.eft_queue[target_executor].put(
                max(earliest_ft, est) + dispatch_latency + execution_time
            )
            task_record["predict_eft"] = (
                max(earliest_ft, est) + dispatch_latency + execution_time
            )

    def advance_schedule(self):
        batch_size = 5000
        cur_qsize = graphHelper.task_queue.qsize()

        if (
            cur_qsize >= batch_size
            or time.time() - graphHelper.get_task_idle_since() > 5
        ):
            # rank the resource by the number of ideal max workers
            cur_resource = self.resource_poller.get_real_time_status()
            resource_rank = list(cur_resource.keys())

            def get_ideal_worker_nums(label):
                return cur_resource[label]["max_cores"]

            resource_rank.sort(key=get_ideal_worker_nums, reverse=True)
            cur_qsize = graphHelper.task_queue.qsize()
            entry_qsize = graphHelper.entry_task_queue.qsize()
            partition_res = self.partition_by_ideal_worker_nums(
                cur_qsize, resource_rank
            )
            cur_vis = 1
            while 0 < entry_qsize:
                head_task = graphHelper.entry_task_queue.get()
                head_fu = head_task["app_fu"]
                cur_vis = self.dfs_on_raw_graph(
                    head_fu, cur_vis, resource_rank, partition_res
                )
                entry_qsize -= 1
            while cur_qsize > 0:
                task_record = graphHelper.task_queue.get()
                if task_record.get("visited") is not True:
                    graphHelper.entry_task_queue.put(task_record)
                    graphHelper.task_queue.put(task_record)
                    graphHelper.update_task_idle_since()
                    continue
                if task_record["status"] == States.scheduling:
                    if len(task_record["depends"]) <= 0:
                        self._stat_eft_info_before_managing(task_record)
                        task_record["status"] = States.data_managing
                        self.data_manager.handle_task_record_data_managing(task_record)
                    else:
                        task_record["status"] = States.dynamic_adjust
                cur_qsize -= 1
        time.sleep(5)

    def partition_by_ideal_worker_nums(self, tot_tasks, resource_rank):
        import math

        ideal_worker_nums = []
        real_time_status = self.resource_poller.get_real_time_status()
        for resource in resource_rank:
            ideal_worker_nums.append(real_time_status[resource]["total_workers"])
        tot_workers = sum(ideal_worker_nums)
        partition_res = [
            math.ceil(tot_tasks * worker / tot_workers) for worker in ideal_worker_nums
        ]
        if sum(partition_res) > tot_tasks:
            redundancy = sum(partition_res) - tot_tasks
            for i in range(len(partition_res)):
                if partition_res[i] - redundancy > 0:
                    partition_res[i] -= redundancy
                    break
                else:
                    redundancy -= partition_res[i]
                    partition_res[i] = 0
        return partition_res

    def dfs_on_raw_graph(self, cur_node, cur_vis, resource_rank, partition_res):
        """
        cur_vis: current visited times
        tot_vis_times: total visited times
        """
        task_record = graphHelper.future_to_task[cur_node]
        if task_record.get("visited") is not None and task_record["visited"] is True:
            return cur_vis
        prefix_sum = partition_res[0]
        for i in range(len(resource_rank)):
            if cur_vis <= prefix_sum:
                if task_record["status"] == States.scheduling:
                    if not task_record["never_change"]:
                        task_record["executor"] = resource_rank[i]
                break
            elif cur_vis > prefix_sum and i < len(resource_rank) - 1:
                prefix_sum += partition_res[i + 1]
            else:
                if task_record["status"] == States.scheduling:
                    if not task_record["never_change"]:
                        task_record["executor"] = resource_rank[i]
        task_record["modified_times"] += 1
        exp_logger.debug(
            f"{task_record['id']} is assigned to {task_record['executor']} with {task_record['modified_times']}"
        )
        if int(task_record["id"]) % 100 == 1:
            exp_logger.debug(
                f"[MicroExp] end scheduling, task {task_record['id']} at time {time.time()}"
            )
        task_record["visited"] = True
        self.assign_same_executor_with_compressed_task(task_record)
        cur_vis += 1
        if cur_node in graphHelper.raw_graph.keys():
            for val in graphHelper.raw_graph[cur_node]:
                cur_vis = self.dfs_on_raw_graph(
                    val, cur_vis, resource_rank, partition_res
                )
        return cur_vis
    

    def check_all_deps_finished(self, task_record):
        all_done = True
        # if task_record is the target
        if task_record['compress_option'][3] is not None:
            for dep in task_record["depends"]:
                if isinstance(dep, Future) and dep.done():
                    continue
                elif isinstance(dep, Future) and not dep.done():
                    dep_task = dep.task_def
                    if dep_task['compress_option'][2] is not None and dep_task['depends'][0].done():
                        continue
                    else:
                        all_done = False
                        break
        else:
            for dep in task_record["depends"]:
                if isinstance(dep, Future) and not dep.done():
                    all_done = False
                    break
        return all_done

    def dynamic_adjust(self, task_record, task_res):
        if self.scheduling_strategy == "RANDOM" or self.scheduling_strategy == "DFS":
            appfu = task_record["app_fu"]
            child_task_fu_list = self.raw_graph[appfu]
            child_task_list = [self.fu_to_task[fu] for fu in child_task_fu_list]
            for child in child_task_list:
                with child["task_launch_lock"]:
                    if child["status"] == States.dynamic_adjust:
                        child["status"] = States.data_managing
                    self.data_manager.handle_task_record_data_managing(
                        child, input_data=task_res, invoke=True
                    )
        elif self.scheduling_strategy == "ADVANCE":
            """
            -------------------------------------------------------------------------------
            WARNING: This code has been deprecated and not be tested in production.
                    It remains here for future updates. 
            -------------------------------------------------------------------------------
            """
            tmp_exe = task_record["executor"]
            # remove one element from the eft_queue
            if not self.eft_queue[tmp_exe].empty():
                self.eft_queue[tmp_exe].get()
            appfu = task_record["app_fu"]
            child_task_fu_list = self.raw_graph[appfu]
            child_task_list = [self.fu_to_task[fu] for fu in child_task_fu_list]
            for child in child_task_list:
                with child["task_launch_lock"]:
                    if child["status"] == States.dynamic_adjust:
                        if "predict_execution" not in child.keys():
                            self.execution_predictor.predict(
                                child
                            )  # predict_execution, predict_output
                        resource = self.resource_poller.get_real_time_status()
                        if (
                            resource[child["executor"]]["idle_workers"] <= 0
                            and resource[child["executor"]]["pending_tasks"] >= 1
                        ):
                            self._advance_relocate_task(child)
                        self._stat_eft_info_before_managing(child)
                        child["status"] = States.data_managing
                    self.data_manager.handle_task_record_data_managing(
                        child, input_data=task_res, invoke=True
                    )
        elif (
            self.scheduling_strategy == "HEFT"
            or self.dynamic_adjust_strategy == "GREEDY"
        ):
            appfu = task_record["app_fu"]
            child_task_fu_list = self.raw_graph[appfu]
            child_task_list = [self.fu_to_task[fu] for fu in child_task_fu_list]
            for child in child_task_list:
                with child["task_launch_lock"]:
                    if child["status"] == States.dynamic_adjust:
                        child["status"] = States.data_managing
                    self.data_manager.handle_task_record_data_managing(
                        child, input_data=task_res, invoke=True
                    )
        elif (
            self.scheduling_strategy == "DATA"
            or self.dynamic_adjust_strategy == "DATA"
            or self.dynamic_adjust_strategy == "GREEDY"
        ):
            appfu = task_record["app_fu"]
            child_task_fu_list = self.raw_graph[appfu]
            child_task_list = [self.fu_to_task[fu] for fu in child_task_fu_list]
            for child in child_task_list:
                # all_done = True
                # for parent in child["depends"]:
                #     if isinstance(parent, Future) and not parent.done():
                #         all_done = False
                #         break

                all_done = self.check_all_deps_finished(child)
                if all_done:
                    # send to DATA_ready_queue
                    if self.dynamic_adjust_strategy == "GREEDY":
                        self.ep_selection.put_task_record(child)
                    else:
                        succ = self._DATA_put_compress_task_to_que(child)
                        
                        #decompress task will be handled later
                        if child['compress_option'][2] is not None:
                            target_app = graphHelper.decompress_to_target_tbl[child['app_fu']]
                            target_task= target_app.task_def
                            is_target_dep_done = self.check_all_deps_finished(target_task)
                            if is_target_dep_done:
                                self.put_task_to_data_ready_queue(target_task)

                            continue

                        if not succ:
                            self.put_task_to_data_ready_queue(child)

        elif self.scheduling_strategy == "AUTO" or self.scheduling_strategy == "NO_OP":
            self.auto_scheduling.handler(task_record, task_res)
        elif self.scheduling_strategy == "FIFO":
            appfu = task_record["app_fu"]
            child_task_fu_list = self.raw_graph[appfu]
            child_task_list = [self.fu_to_task[fu] for fu in child_task_fu_list]
            for child in child_task_list:
                all_done = True
                for parent in child["depends"]:
                    if isinstance(parent, Future) and not parent.done():
                        all_done = False
                        break
                if all_done:
                    # send to DATA_ready_queue
                    self.FIFO_ready_queue.put(child)

        return

    def _advance_relocate_task(self, task_record):
        exp_logger.info(f"Ready to relocate task: {task_record['id']}")
        dispatch_latency = 3
        original_exe = task_record["executor"]
        cur_time = time.time()
        estimated_est = (
            self.eft_queue[original_exe].get()
            if not self.eft_queue[original_exe].empty()
            else cur_time
        )
        if estimated_est is not cur_time:
            self.eft_queue[original_exe].put(estimated_est)
        estimated_eft = (
            max(
                estimated_est,
                self.transfer_predictor.predict_est_for_certain_executor(
                    task_record, original_exe
                ),
            )
            + task_record["predict_execution"][original_exe]
            + dispatch_latency
        )
        resource = self.resource_poller.get_real_time_status()
        # find the best resource
        possible_resource = []
        for key in resource.keys():
            if (
                resource[key]["idle_workers"] > 0
                and resource[key]["pending_tasks"] <= 0
            ):
                possible_resource.append(key)

        if len(possible_resource) == 0:
            return

        changed_resource = original_exe

        for resource in possible_resource:
            est = max(
                cur_time,
                self.transfer_predictor.predict_est_for_certain_executor(
                    task_record, resource
                ),
            )
            eft = est + task_record["predict_execution"][resource] + dispatch_latency
            exp_logger.info(f"eft {eft} and  estimated_eft {estimated_eft}")
            if eft < estimated_eft:
                estimated_eft = eft
                changed_resource = resource
        if not task_record["never_change"]:
            task_record["executor"] = changed_resource
        logging.info(
            "Relocate task {} from {} to {}".format(
                task_record["id"], original_exe, changed_resource
            )
        )
        exp_logger.info(
            "Relocate task {} from {} to {}".format(
                task_record["id"], original_exe, changed_resource
            )
        )


class TaskWithTransferSize:
    def __init__(self, task, target_ep, transfer_predictor):
        self.task_record = task
        self.target_ep = target_ep
        self.transfer_size = transfer_predictor.caculate_transfer_size(task, target_ep)

    def __lt__(self, other):
        return self.transfer_size < other.transfer_size
