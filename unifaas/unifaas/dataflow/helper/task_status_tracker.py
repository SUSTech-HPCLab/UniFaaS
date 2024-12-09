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
    def __init__(self, scheduling_method=None):
        self.ready_tasks = 0
        self.not_ready_tasks = 0
        self.executed_tasks = 0
        self.running_tasks = 0
        self.ready_to_launch_tasks = 0
        # report status per {report_interval} seconds
        self.report_interval = 3
        self.scheduling_method = scheduling_method
        self.report_thread = threading.Thread(target=self.report_status)
        self.report_thread.daemon = True
        self.report_thread.start()
        self.dheft_prepare_compress = 0
        self.dheft_waiting_decompress = 0

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
            if self.scheduling_method != "DHEFT":
                exp_logger.info(
                    f"[TaskkStatusTracker] ready: {self.ready_tasks}, not ready: {self.not_ready_tasks}, executed: {self.executed_tasks}, running: {self.running_tasks}, ready to launch: {self.ready_to_launch_tasks}"
                )
            else:
                exp_logger.info(
                    f"[TaskkStatusTracker] ready: {self.ready_tasks}, not ready: {self.not_ready_tasks}, dheft_prepare_compress: {self.dheft_prepare_compress}, dheft_waiting_decompress: {self.dheft_waiting_decompress}, executed: {self.executed_tasks}, running: {self.running_tasks}, ready to launch: {self.ready_to_launch_tasks}"
                )

    def update_when_submit_to_dfk(self, task_record):
        if task_record["compress_option"][1] or task_record["compress_option"][2] or 'special_transfer_task' in task_record:
            return
        
        ready = self.check_if_ready(task_record)
        if ready:
            self.ready_tasks += 1
        else:
            self.not_ready_tasks += 1


    def update_when_task_submit_to_executor(self, task_record):
        if task_record["compress_option"][1] is not None or task_record["compress_option"][2] is not None or 'special_transfer_task' in task_record:
            return

        if "original_task" in task_record:
            return
        else:
            self.running_tasks += 1
            self.ready_tasks -= 1

    def update_when_task_done(self, task_record):
        if task_record["compress_option"][1] is None and  task_record["compress_option"][2] is None and  'special_transfer_task' not in task_record:
            self.running_tasks -= 1
            self.executed_tasks += 1

        if "original_task" in task_record:
            return

        if self.scheduling_method != "DHEFT":
            if task_record["app_fu"] in graphHelper.raw_graph:
                # DHEFT的压缩任务不在raw_graph里，但是DATA类型的压缩任务在raw_graph里
                child_tasks = graphHelper.raw_graph[task_record["app_fu"]]
                for child in child_tasks:
                    child_record = child.task_def
                    if child_record["compress_option"][1] or child_record["compress_option"][2]:
                        continue

                    if self.dep_count(child_record) == 1:
                        self.ready_tasks += 1
                        self.not_ready_tasks -= 1

    def modify_ready_to_launch(self, num):
        self.ready_to_launch_tasks += num

    def dheft_update_when_dep_finished(self):
        self.not_ready_tasks -= 1
        self.dheft_prepare_compress += 1

    def dheft_update_when_dep_need_compress(self, task_record):
        if len(task_record['depends']) > 0:
            self.dheft_prepare_compress -= 1
        else:
            self.ready_tasks -= 1


        self.dheft_waiting_decompress += 1
    
    def dheft_update_when_dep_not_need_compress(self, task_record):
        if len(task_record['depends']) > 0:
            self.dheft_prepare_compress -= 1
        else:
            self.ready_tasks -= 1

        self.ready_tasks += 1

    def dheft_update_when_decompress_done(self):
        self.dheft_waiting_decompress -= 1
        self.ready_tasks += 1
