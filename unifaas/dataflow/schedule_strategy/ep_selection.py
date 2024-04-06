from unifaas.dataflow.states import States
import threading
import time
import math
import logging
from concurrent.futures import Future
from queue import PriorityQueue
from queue import Queue
import random

exp_logger = logging.getLogger("experiment")


class TaskWithPriority:
    def __init__(self, task):
        self.task_record = task
        if "heft_priority" in task.keys():
            self.priority = task["heft_priority"]
        elif "ic_priority" in task.keys():
            self.priority = task["ic_priority"]
        else:
            self.priority = 0

    def __lt__(self, other):
        return self.priority > other.priority


class GreedySelection:
    def __init__(
        self,
        resource_poller,
        execution_predictor,
        transfer_predictor,
        data_manager,
        priority_type,
    ):
        self.resource_poller = resource_poller
        self.execution_predictor = execution_predictor
        self.transfer_predictor = transfer_predictor
        self.data_manager = data_manager
        self.endpoint_performance_ratio = None
        self.priority_type = priority_type
        self.scheduling_queue = Queue()  # scheduling queue
        self.data_ready_queue = PriorityQueue()  # ic_optimal priority queue

    def update_performance_ratio(self, ratio_result):
        if self.endpoint_performance_ratio is None:
            self.endpoint_performance_ratio = ratio_result

    def put_task_record(self, task_record):
        task_with_priority = TaskWithPriority(task_record)
        self.data_ready_queue.put(task_with_priority)

    def fetch_task_record(self):
        if self.data_ready_queue.empty():
            return None
        else:
            return self.data_ready_queue.get().task_record

    def select_endpoint(self, task_record, feasible_endpoints):
        transfer_cost = 0
        eft_dict = {}
        for ep in feasible_endpoints:
            eft_dict[
                ep
            ] = self.transfer_predictor.real_time_predict_comm_cost_for_task_record(
                task_record, ep
            )
            eft_dict[ep] += self.resource_poller.calculate_ideal_execution_duration(
                ep, task_record
            )
        return min(eft_dict, key=eft_dict.get)

    def _handle_data_ready(self):
        if self.data_ready_queue.qsize() > 0:
            exp_logger.info(
                f"handle data ready with qsize {self.data_ready_queue.qsize()}"
            )
        while True:
            feasible_ep = []
            real_time_resource = self.resource_poller.get_real_time_status()
            for key in real_time_resource.keys():
                if real_time_resource[key]["total_workers"] == 0:
                    continue
                ratio_factor = 1
                if (
                    self.endpoint_performance_ratio is not None
                    and key in self.endpoint_performance_ratio.keys()
                ):
                    ratio_factor = self.endpoint_performance_ratio[key]
                # no need to check pending tasks
                # total_pending_tasks_upper_bound = real_time_resource[key]['total_workers']*0.25 * ratio_factor # real_time_resource[key]['total_workers'] + real_time_resource[key]['total_workers']*0.25 * ratio_factor
                # if real_time_resource[key]['pending_tasks'] < total_pending_tasks_upper_bound:
                #     feasible_ep.append(key)
                feasible_ep.append(key)
            if len(feasible_ep) == 0:
                time.sleep(0.5)
                continue

            task_record = self.fetch_task_record()
            if task_record is None:
                break
            if "heft_priority" in task_record.keys():
                exp_logger.debug(
                    f"[DHEFT] task {task_record['id']} {task_record['func_name']} with priority {task_record['heft_priority']} "
                )

            if task_record["status"] == States.scheduling:
                try:
                    for dep in task_record["depends"]:
                        if isinstance(dep, Future):
                            dep.result()
                except Exception as e:
                    exp_logger.warn(
                        f"task {task_record['id']} failed with exception {e}"
                    )
                    task_record["status"] = States.pending
                    continue

                target_ep = self.select_endpoint(task_record, feasible_ep)
                if not task_record["never_change"]:
                    task_record["executor"] = target_ep
                self.resource_poller.update_status_when_submit_one_task(
                    target_ep, task_record=task_record
                )
                task_record["submitted_to_poller"] = True
                task_record["status"] = States.data_managing
                self.data_manager.group_transfer(task_record)
        time.sleep(
            7
        )  # sleep for 7 seconds to avoid too short interval between two scheduling

    def assign_for_queue(self, task_queue):
        while not task_queue.empty():
            task_record = task_queue.get()
            if task_record["status"] == States.scheduling:
                all_done = True
                for parent in task_record["depends"]:
                    if isinstance(parent, Future) and not parent.done():
                        all_done = False
                        break
                if all_done:
                    self.put_task_record(task_record)
        self._handle_data_ready()
