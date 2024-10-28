from unifaas.dataflow.states import States
import time
import logging
from concurrent.futures import Future
from queue import PriorityQueue
from queue import Queue
from unifaas.dataflow.helper.graph_helper import graphHelper

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
        self.compress_task_queue = Queue() # speciallized queue for compression task
        self.compress_target_task_que = Queue() # target task queue
        self.tmp_dfk   = None
     

    def update_performance_ratio(self, ratio_result):
        if self.endpoint_performance_ratio is None:
            self.endpoint_performance_ratio = ratio_result

    def put_task_record(self, task_record):
        if task_record['compress_option'][1] is not None:
            self.compress_task_queue.put(task_record)
            return

        task_with_priority = TaskWithPriority(task_record)
        self.data_ready_queue.put(task_with_priority)

    def fetch_task_record(self):
        if not self.compress_task_queue.empty():
            return self.compress_task_queue.get()

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

            # secondly submit compress target task periodically
            cur_target_size = self.compress_target_task_que.qsize()
            while cur_target_size > 0:
                task_record = self.compress_target_task_que.get()
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
                    self.compress_target_task_que.put(task_record)
                cur_target_size -= 1

            task_record = self.fetch_task_record()
            if task_record is None:
                break
            if "heft_priority" in task_record.keys():
                exp_logger.debug(
                    f"[DHEFT] task {task_record['id']} {task_record['func_name']} with priority {task_record['heft_priority']} "
                )

            if task_record["status"] == States.scheduling:
    
   
                if task_record["compress_option"][1] is None and task_record["compress_option"][2] is None:  
                    target_ep = self.select_endpoint(task_record, feasible_ep) #don't select endpoint for a compress task
                    if not task_record["never_change"]:
                        task_record["executor"] = target_ep

                if task_record["compress_option"][1] is not None:
                    self.schedule_compress_task(task_record['app_fu'])
                
                if task_record["compress_option"][3] is not None:
                    self.invoke_decompress_task(task_record)
                    self.append_compression_if_necessary(task_record)
                    continue

                
                self.append_compression_if_necessary(task_record)
                self.resource_poller.update_status_when_submit_one_task(
                    task_record['executor'], task_record=task_record
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

    def schedule_compress_task(self, compress_app):
        task_record = compress_app.task_def
        # compress task has only one dep
        if len(task_record['depends']) != 1:
            exp_logger.error("Compression task has abnormal depends")
        source_task  = task_record['depends'][0].task_def
        task_record['executor'] = source_task['executor']
        

    def invoke_decompress_task(self,task_record):
        if task_record['compress_option'][3] is None:
            raise RuntimeError("Don't put a non compression target task into the queue")
        self.compress_target_task_que.put(task_record)
        for dep in task_record['depends']:
            # launch all decompress task for a target task
            if not dep.done():
                dep_task = dep.task_def
                if dep_task['compress_option'][2] is not None and (dep_task["status"] == States.scheduling or dep_task["status"] == States.dynamic_adjust):
                    dep_task['executor'] = task_record['executor']
                    self.resource_poller.update_status_when_submit_one_task(dep_task['executor'])
                    dep_task["submitted_to_poller"] = True
                    dep_task["status"] = States.data_managing
                    self.data_manager.group_transfer(dep_task)

    def append_compression_if_necessary(self, task_record):
        # TODO: 何时调用，何时算necessary还没有实现
        if self.tmp_dfk is None:
            from unifaas.dataflow.dflow import DataFlowKernelLoader
            self.tmp_dfk = DataFlowKernelLoader.dfk()

        
        if task_record['compress_option'][0] or task_record['compress_option'][1] is not None or task_record['compress_option'][2] is not None:
            # 不重复压缩 已下达压缩命令的，压缩任务，解压任务
            return
                
        # compress all 暂时先全部压缩 TODO 需要判断什么时候压缩
        if task_record['func_name'] is not None:
            # select a compression method
            if task_record['compress_option'][3] is not None:
                task_record['compress_option'] = ('gzip', None, None,task_record['compress_option'][3])
            else:
                task_record['compress_option'] = ('gzip', None, None,None)
            compress_app = self.tmp_dfk.append_compress_task(task_record, task_record['app_fu'], internal_submit=True)

            children_copy = []
            decompress_app_list = []
            # change all influenced target app
            for fu in graphHelper.raw_graph[task_record['app_fu']]:
                decompress_app= self.tmp_dfk.append_decompress_task(compress_app, task_record['app_fu'], internal_submit=True)
                child_task = fu.task_def
                for i in range(len(child_task['depends'])):
                    if child_task['depends'][i] == task_record['app_fu']:
                        child_task['depends'][i] = decompress_app

                app_args, app_kwargs = self.tmp_dfk.replace_args_and_kwargs(child_task['args'],child_task['kwargs'])
                child_task['args'] = app_args
                child_task['kwargs'] = app_kwargs
                children_copy.append(fu)
                decompress_app_list.append(decompress_app)
                graphHelper.decompress_to_target_tbl[decompress_app] = child_task['app_fu']
                if child_task['compress_option'][0] is None:
                    child_task['compress_option'] = (None,None,None,True) # src -> compress -> decompress -> target
                else:
                    child_task['compress_option'] = (child_task['compress_option'][0],None,None,True)
                graphHelper.raw_graph[decompress_app] = [fu]
            
            #handle DAG structure
            graphHelper.raw_graph[task_record['app_fu']] = [compress_app]
            graphHelper.raw_graph[compress_app] = decompress_app_list

            self.schedule_compress_task(compress_app)
