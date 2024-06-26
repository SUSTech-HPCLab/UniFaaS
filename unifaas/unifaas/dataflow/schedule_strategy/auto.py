import threading
import time
from unifaas.dataflow.helper.graph_helper import graphHelper
from unifaas.dataflow.data_transfer_management import DataTransferManager
from unifaas.dataflow.states import States, FINAL_STATES
from concurrent.futures import Future
from queue import Queue, PriorityQueue
from unifaas.dataflow.schedule_strategy.heft import heft_schedule_entry
from concurrent.futures import Future

import logging

exp_logger = logging.getLogger("experiment")


class TaskWithTransferSize:
    def __init__(self, task, target_ep):
        self.task_record = task
        self.target_ep = target_ep
        self.transfer_size = self._count_transfer_size(task)

    def _count_transfer_size(self, task_record):
        transfer_size = 0
        for dep in task_record["depends"]:
            executor = dep.task_def["executor"]
            if not self.target_ep == executor:
                transfer_size += dep.task_def["output_size"]
        return transfer_size

    def __lt__(self, other):
        return self.transfer_size < other.transfer_size


class AutoScheduling:
    def __init__(
        self,
        resource_poller,
        execution_predictor,
        transfer_predictor,
        data_transfer_manager,
    ):
        self.resource_poller = resource_poller
        self.execution_predictor = execution_predictor
        self.transfer_predictor = transfer_predictor
        self.data_transfer_manager = data_transfer_manager
        self.func_dict = {}
        self.auto_data_locality_queue = Queue()
        self.data_ready_queue_dict = {}
        self.resources = self.resource_poller.get_real_time_status()
        for ep in self.resources.keys():
            self.data_ready_queue_dict[ep] = PriorityQueue()
        self.sub_graph_nodes = set()  # nodes that be executed in DATA scheduling
        self.chosen_strategy = None
        self.processing_stage = (
            0  # 0 means data locality scheduling, 1 means heft scheduling
        )

    def _analyze_workflow(self, workflow_graph, id_to_task):
        self.workflow_graph = workflow_graph
        self.id_to_task = id_to_task
        for node in workflow_graph:
            node_task = id_to_task[node]
            if node_task["func_name"] not in self.func_dict:
                self.func_dict[node_task["func_name"]] = {
                    "task_num": 0,
                    "completed_num": 0,
                }
            self.func_dict[node_task["func_name"]]["task_num"] += 1
        # Check the integrity of workflow information
        in_history, out_history = self.execution_predictor.look_up_history_model(
            self.func_dict.keys()
        )

        # TODO only for debugging!
        out_history = ["prepare_files_and_can"]

        if len(out_history) == 0:
            # full knowledge of workflow
            return "HEFT"
        elif len(in_history) == 0:
            # no knowledge of workflow
            return "DATA"
        else:
            # partial knowledge of workflow
            # extracts tasks that can be executed immediately with unknown knowledge
            unknown_tasks = []
            for task_id in workflow_graph:
                task_record = id_to_task[task_id]
                if task_record["func_name"] in out_history:
                    unknown_tasks.append(task_id)

            # Ensure that unknown tasks can be started directly
            no_dep = True
            for task_id in unknown_tasks:
                task_record = id_to_task[task_id]
                for dep in task_record["depends"]:
                    if dep.task_def["id"] not in unknown_tasks:
                        no_dep = False
                        break
            if not no_dep:
                return "DATA"

            # Find tasks which are at the same level of unknown tasks
            levels = graphHelper.analyze_level(workflow_graph)
            for level in levels:
                if len(set(level).intersection(set(unknown_tasks))) > 0:
                    for task_id in level:
                        task_record = id_to_task[task_id]
                        self.auto_data_locality_queue.put(task_record)
                        self.sub_graph_nodes.add(task_id)
            exp_logger.info("Unknown tasks: {}".format(len(self.sub_graph_nodes)))
            return "PART"

    def _AUTO_put_task_to_data_ready_queue(self, task_record):
        for executor in self.resources.keys():
            task_with_transfer_size = TaskWithTransferSize(task_record, executor)
            self.data_ready_queue_dict[executor].put(task_with_transfer_size)
        return

    def _AUTO_fetch_minimum_transfer_task(self, executor):
        ready_q = self.data_ready_queue_dict[executor]
        while not ready_q.empty():
            task_with_transfer_size = ready_q.get()
            if task_with_transfer_size.task_record["status"] == States.scheduling:
                return task_with_transfer_size.task_record
        return None

    def AUTO_data_locality(self):
        while True:
            while not self.auto_data_locality_queue.empty():
                task_record = self.auto_data_locality_queue.get()
                if task_record["status"] == States.scheduling:
                    all_done = True
                    for parent in task_record["depends"]:
                        if isinstance(parent, Future) and not parent.done():
                            all_done = False
                            break
                    if all_done:
                        self._AUTO_put_task_to_data_ready_queue(task_record)
            self._AUTO_handle_ready()
            for task_id in self.sub_graph_nodes.copy():
                task_record = self.id_to_task[task_id]
                if task_record["status"] < States.data_managing:
                    self.auto_data_locality_queue.put(task_record)
                elif task_record["status"] in FINAL_STATES:
                    self.sub_graph_nodes.remove(task_id)
            if len(self.sub_graph_nodes) == 0:
                self.processing_stage = 1
                break
        exp_logger.info("Data locality scheduling finished")

    def _AUTO_handle_ready(self):
        cur_resource = self.resource_poller.get_real_time_status()
        idle_workers = []
        endpoints = []
        for ep in cur_resource.keys():
            idle_workers.append(cur_resource[ep]["idle_workers"])
            endpoints.append(ep)
        attempt_times = sum(idle_workers)
        while attempt_times > 0:
            for i in range(len(idle_workers)):
                target_ep = endpoints[i]
                if idle_workers[i] > 0:
                    task_record = self._AUTO_fetch_minimum_transfer_task(target_ep)
                    if task_record is None:
                        continue
                    if not task_record["never_change"]:
                        task_record["executor"] = target_ep
                    self.resource_poller.update_status_when_submit_one_task(target_ep)
                    task_record["submitted_to_poller"] = True
                    task_record["status"] = States.data_managing
                    self.data_transfer_manager.group_transfer(task_record)
                    idle_workers[i] -= 1
                    break
            attempt_times -= 1

    def select_strategy(self):
        if self.chosen_strategy:
            return self.chosen_strategy

        if not graphHelper.get_busy_status():
            workflow_graph, id_to_task = graphHelper.get_workflow_graph()
            if len(workflow_graph.keys()) == 0:
                return "AUTO"

            strategy = self._analyze_workflow(workflow_graph, id_to_task)
            self.chosen_strategy = strategy
            exp_logger.info("AUTO Chosen strategy: %s" % strategy)
            return strategy

    def AUTO_heft_schedule(self):
        # analyze the tasks have not been executed
        un_executed_tasks = []
        for key in self.id_to_task.keys():
            if self.id_to_task[key]["status"] not in FINAL_STATES:
                un_executed_tasks.append(key)

        pure_dag = {}
        fu_to_task = {}
        for key in un_executed_tasks:
            parent_appfu = self.id_to_task[key]["app_fu"]
            fu_to_task[parent_appfu] = self.id_to_task[key]
            if parent_appfu not in pure_dag:
                pure_dag[parent_appfu] = []
            for child in self.workflow_graph[key]:
                child_appfu = self.id_to_task[child]["app_fu"]
                if child_appfu not in pure_dag[parent_appfu]:
                    pure_dag[parent_appfu].append(child_appfu)

        machines = self.resource_poller.get_resource_status().keys()
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

        for fu in allocate_res.keys():
            task_record = fu_to_task[fu]
            not_done_deps = [dep for dep in task_record["depends"] if not dep.done()]
            if len(not_done_deps) <= 0:
                task_record["status"] = States.data_managing
                if not task_record["never_change"]:
                    task_record["executor"] = allocate_res[fu]
                task_record["confirm_deps_done"] = True
                self.data_transfer_manager.handle_task_record_data_managing(task_record)
            else:
                task_record["status"] = States.dynamic_adjust
                if not task_record["never_change"]:
                    task_record["executor"] = allocate_res[fu]

    def handler(self, task_record, task_res):
        appfu = task_record["app_fu"]
        task_id = task_record["id"]
        # handle the task in the subgraph
        child_task_id_list = self.workflow_graph[task_id]
        if self.processing_stage == 0:
            for child in child_task_id_list:
                if child not in self.sub_graph_nodes:
                    continue
                child_task = self.id_to_task[child]
                all_done = True
                for parent in child_task["depends"]:
                    if isinstance(parent, Future) and not parent.done():
                        all_done = False
                        break
                if all_done:
                    # send to DATA_ready_queue
                    self._AUTO_put_task_to_data_ready_queue(child_task)
        else:
            for child in child_task_id_list:
                child_task = self.id_to_task[child]
                with child_task["task_launch_lock"]:
                    if child_task["status"] == States.dynamic_adjust:
                        child_task["status"] = States.data_managing
                    self.data_transfer_manager.handle_task_record_data_managing(
                        child_task, input_data=task_res, invoke=True
                    )

        return
