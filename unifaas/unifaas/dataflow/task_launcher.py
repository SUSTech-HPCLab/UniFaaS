from unifaas.dataflow.helper.resource_status_poller import ResourceStatusPoller
from collections import deque
from unifaas.dataflow.states import States
from unifaas.dataflow.data_transfer_management import DataTransferManager
from unifaas.dataflow.helper.transfer_predictor import TransferPredictor
from unifaas.executors import FuncXExecutor
import logging

exp_logger = logging.getLogger("experiment")


class TaskLauncher:
    def __init__(
        self, executors, launch_function, scheduling_strategy, task_status_tracker
    ):
        self.executors = executors
        self.scheduling_strategy = scheduling_strategy
        self.launch_function = launch_function
        self.resource_poller = ResourceStatusPoller(self.executors)
        self.data_manager = DataTransferManager(self.executors)
        self.transfer_predictor = TransferPredictor(self.executors)
        self.task_status_tracker = task_status_tracker
        self.launch_que_dict = {}
        for executor in executors:
            if isinstance(executors[executor], FuncXExecutor):
                self.launch_que_dict[executor] = deque()

    def dispatch_task_record_to_launch_que(self, task_record):
        executor = task_record["executor"]
        task_record["status"] = States.queuing
        self.task_status_tracker.modify_ready_to_launch(1)
        self.launch_que_dict[executor].appendleft(task_record)

    def no_block_launch(self):
        for key in self.launch_que_dict.keys():
            dq = self.launch_que_dict[key]
            while len(dq) > 0:
                task_record = dq.pop()
                self.resource_poller.update_when_launch_task(
                    key, task_record=task_record
                )
                self.task_status_tracker.modify_ready_to_launch(-1)
                self.launch_function(task_record)

    def change_dest_executor(self, task_record, dest_executor):
        orginal_executor = task_record["executor"]
        # clean-up effect to execution time counter
        self.resource_poller.release_being_executed_time(orginal_executor, task_record)
        self.resource_poller.modify_launch_queue_task_num(orginal_executor, -1)
        exp_logger.info(
            f"[TaskLauncher] task {task_record['id']} is moved from {orginal_executor} to {dest_executor}"
        )
        self.task_status_tracker.modify_ready_to_launch(-1)
        task_record["executor"] = dest_executor
        task_record["changed_executor"] = True
        self.resource_poller.update_status_when_submit_one_task(
            dest_executor, task_record=task_record
        )
        task_record["submitted_to_poller"] = True
        task_record["status"] = States.data_managing
        self.data_manager.group_transfer(task_record)

    def block_launch(self):
        # if there are enough resources, launch them
        for key in self.launch_que_dict.keys():
            dq = self.launch_que_dict[key]
            resource_status = self.resource_poller.get_real_time_status()
            while len(dq) > 0:
                cur_running_task_num = (
                    resource_status[key]["total_workers"]
                    - resource_status[key]["idle_workers"]
                    + resource_status[key]["pending_tasks"]
                )
                if cur_running_task_num < resource_status[key]["total_workers"] * 1.25:
                    task_record = dq.pop()
                    self.resource_poller.update_when_launch_task(
                        key, task_record=task_record
                    )
                    self.task_status_tracker.modify_ready_to_launch(-1)
                    self.launch_function(task_record)
                else:
                    break

        # check if there are tasks that can be moved to other executors
        for key in self.launch_que_dict.keys():
            dq = self.launch_que_dict[key]
            if len(dq) == 0:
                continue

            upper_bound_iter = 0
            for possible_ep in self.launch_que_dict.keys():
                if key != possible_ep:
                    upper_bound_iter += max(
                        10,
                        resource_status[possible_ep]["idle_workers"]
                        - resource_status[possible_ep]["pending_tasks"]
                        - resource_status[possible_ep]["launch_queue_task_num"],
                    )
            tmp_deque = deque()
            while upper_bound_iter > 0:
                if len(dq) == 0:
                    break
                resource_status = self.resource_poller.get_real_time_status()
                feasible_ep = []
                for possible_ep in self.launch_que_dict.keys():
                    if key != possible_ep:
                        # tmp
                        # if resource_status[possible_ep]['total_workers'] -  resource_status[possible_ep]['idle_workers'] + \
                        #     resource_status[possible_ep]['pending_tasks'] + resource_status[possible_ep]['launch_queue_task_num']  < resource_status[possible_ep]['total_workers']:
                        #     feasible_ep.append(possible_ep)
                        feasible_ep.append(possible_ep)

                if len(feasible_ep) == 0:
                    break
                task_record = dq.pop()
                if "changed_executor" in task_record.keys():
                    if task_record["changed_executor"]:
                        tmp_deque.appendleft(task_record)
                        upper_bound_iter -= 1
                        continue
                cur_cost = self.resource_poller.calculate_ideal_execution_duration(
                    key, task_record
                )
                cur_cost_log = cur_cost
                min_ep = key
                for ep in feasible_ep:
                    # new_cost = self.transfer_predictor.real_time_predict_comm_cost_for_task_record(task_record, ep) + \
                    #     task_record['predict_execution'][ep] tmp
                    new_cost = self.transfer_predictor.real_time_predict_comm_cost_for_task_record(
                        task_record, ep
                    ) + self.resource_poller.calculate_ideal_execution_duration(
                        ep, task_record
                    )
                    if (
                        new_cost < cur_cost * 0.7 and abs(cur_cost - new_cost) > 300
                    ):  # tmp
                        min_ep = ep
                        cur_cost = new_cost
                if min_ep != key:
                    exp_logger.info(
                        f"cur cost {cur_cost_log} higher than new cost {cur_cost}"
                    )
                    self.change_dest_executor(task_record, min_ep)
                else:
                    tmp_deque.appendleft(task_record)
                upper_bound_iter -= 1
            while len(tmp_deque) > 0:
                task_record = tmp_deque.popleft()
                dq.append(task_record)

    def try_launch(self):
        if (
            self.scheduling_strategy == "DFS"
            or self.scheduling_strategy == "DATA"
            or self.scheduling_strategy == "FIFO"
            or self.scheduling_strategy == "RANDOM"
        ):
            self.no_block_launch()
        elif (
            self.scheduling_strategy == "DHEFT" or self.scheduling_strategy == "HYBRID"
        ):
            self.block_launch()
        else:
            raise Exception("Unsupported scheduling strategy")
