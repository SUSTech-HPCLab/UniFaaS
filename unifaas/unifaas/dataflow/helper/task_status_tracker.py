import logging
from concurrent.futures import Future
import threading
import time
from unifaas.dataflow.helper.graph_helper import graphHelper

exp_logger = logging.getLogger("experiment")

"""
This class is designed to analyze the usage of IC-optimal shceduling
"""


class TaskStatusTracker:
    def __init__(self):
        self.ready_tasks = 0
        self.not_ready_tasks = 0
        self.executed_tasks = 0
        self.running_tasks = 0
        self.ready_to_launch_tasks = 0
        # report status per {report_interval} seconds
        self.report_interval = 3
        self.report_thread = threading.Thread(target=self.report_status)
        self.report_thread.daemon = True
        self.report_thread.start()

    def check_if_ready(self, task_record):
        if len(task_record["depends"]) == 0:
            return True
        else:
            for dep in task_record["depends"]:
                if isinstance(dep, Future):
                    if not dep.done():
                        return False
        return True

    def dep_count(self, task_record):
        dep_counter = 0
        for dep in task_record["depends"]:
            if isinstance(dep, Future):
                if not dep.done():
                    dep_counter += 1
        return dep_counter

    def report_status(self):
        while True:
            time.sleep(self.report_interval)
            exp_logger.info(
                f"[TaskkStatusTracker] ready: {self.ready_tasks}, not ready: {self.not_ready_tasks}, executed: {self.executed_tasks}, running: {self.running_tasks}, ready to launch: {self.ready_to_launch_tasks}"
            )

    def update_when_submit_to_dfk(self, task_record):
        ready = self.check_if_ready(task_record)
        if ready:
            self.ready_tasks += 1
        else:
            self.not_ready_tasks += 1

    def update_when_task_submit_to_executor(self, task_record):
        if "original_task" in task_record:
            return
        else:
            self.running_tasks += 1
            self.ready_tasks -= 1

    def update_when_task_done(self, task_record):
        if "original_task" in task_record:
            return
        self.running_tasks -= 1
        self.executed_tasks += 1
        child_tasks = graphHelper.workflow_graph[task_record["id"]]
        for child in child_tasks:
            child_record = graphHelper.id_to_task[child]
            if self.dep_count(child_record) == 1:
                self.ready_tasks += 1
                self.not_ready_tasks -= 1

    def modify_ready_to_launch(self, num):
        self.ready_to_launch_tasks += num
