from dataclasses import dataclass
from typing import Any, Dict, List, Set
from enum import Enum
import heapq
import time
import threading
from unifaas.dataflow.analyzer.mock_analyzer import MockDataManager
from unifaas.dataflow.analyzer.mock_analyzer import MockTaskStates
from unifaas.dataflow.analyzer.mock_analyzer import MockTaskWithPriority
from unifaas.dataflow.analyzer.mock_analyzer import MockSchedulerDataSource
from unifaas.dataflow.analyzer.mock_analyzer import MockAnalyzer


from queue import PriorityQueue
from unifaas.dataflow.analyzer.mock_endpoint import MockEndpoint
from unifaas.dataflow.analyzer.mock_data_manager import MockDataManager
from unifaas.dataflow.helper.graph_helper import graphHelper
from datetime import datetime, timedelta
import copy
from unifaas.dataflow.states import States
from unifaas.dataflow.helper.resource_status_poller import ResourceStatusPoller
from unifaas.dataflow.helper.transfer_predictor import TransferPredictor
from funcx.sdk.file import RemoteFile, RemoteDirectory

class EventType(Enum):
    TASK_COMPLETE = 1
    TRANSFER_COMPLETE = 2


class Event:

    def __init__(self, task_type, start_time, end_time, payload):
        self.type = task_type
        self.start_time = start_time
        self.end_time = end_time
        self.payload = payload

    def __lt__(self, other):
        return self.end_time < other.end_time

class EventSimulator:
    def __init__(self, enabled=False):
        self.enabled = enabled
        self.simulation_thread = None
        self.mock_endpoints = {} 
        self.start_simulation()

    def initialize_events(self, cur_time):
        # init 现在正在进行的数据传输
        # init 现在正在进行的任务
        self.current_time = cur_time
        self.event_queue = [] 
        self.unscheduled_tasks_set = set() # 只在开启模拟时使用
        self.mock_transfer_counter = {}
        self.mock_endpoints_copy = copy.deepcopy(self.mock_endpoints)
        self.mock_task_status_tbl = {} # 这个是必须的，因为需要知道dep的状态
        self.mock_data_manager = MockDataManager()
        self.mock_transfer_counter = {}

        self.mock_transfer_histroy = {} # transfer_id to task_id
     
        for task_app in graphHelper.raw_graph:
            task_record = task_app.task_def
            task_id = task_record['id']
            if task_record['status'] == States.exec_done:
                self.mock_task_status_tbl[task_record['id']] = MockTaskStates.completed
                task_record['simu_executor'] = task_record['executor']
            elif 'added_to_analyzer' in task_record:
                # TODO: 12月25日先不考虑 运行时执行模拟任务的问题
                # 把这些运行中的任务放到 event queue中 （start_time和end_time）
                pass
            else:
                self.mock_task_status_tbl[task_record['id']] = MockTaskStates.not_scheduled
                self.unscheduled_tasks_set.add(task_id)

        for ep in self.mock_endpoints_copy:
            for ep2 in self.mock_endpoints_copy:
                if ep != ep2:
                    cur_transffering_size = self.data_manager.dtc.get_transferring_size(ep, ep2)
                    self.mock_data_manager.init_mock_data_channel(ep, ep2, cur_transffering_size)

        tmp_resource_poller = ResourceStatusPoller(None)
        self.mock_ds = MockSchedulerDataSource(self.data_manager, tmp_resource_poller)
        self.entry_schedule_tasks =  PriorityQueue()  
        tasks_to_remove = set()




        # TODO: 达到运行时执行模拟任务 可能需要修改下面这些代码
        # TODO: 由于是event驱动的，似乎不是很需要unscheduled_tasks_set，直接一个entry list就好了
        for task_id in self.unscheduled_tasks_set:
            if self.check_task_ready_for_data_transfer(task_id):
                self.entry_schedule_tasks.put(MockTaskWithPriority(graphHelper.id_to_task[task_id]))
                tasks_to_remove.add(task_id)

        self.unscheduled_tasks_set -= tasks_to_remove
        
        # 进行入口task的调度，生成events
        while self.entry_schedule_tasks.qsize():
            prio_task = self.entry_schedule_tasks.get()
            task_record = prio_task.task_record
            self.do_schedule_for_task(task_record)




    def do_schedule_for_task(self, task_record):
        mock_to_transfer_map = self.prepare_mock_transfer(task_record)
        task_record["simu_executor"] = self.mock_DHEFT_schedule(task_record, mock_to_transfer_map)
        transfer_cnt = self.do_transfer_for_task(task_record, mock_to_transfer_map)
        if transfer_cnt == 0 :
            self.launch_task(task_record)
        else:
            self.mock_transfer_counter[task_record["id"]] = transfer_cnt
    
    def do_transfer_for_task(self, task_record, mock_to_transfer_map):
        dest_ep = task_record["simu_executor"]
        transfer_cnt = 0
        for src_ep in mock_to_transfer_map:
            if src_ep == dest_ep: 
                continue
            transfer_size = mock_to_transfer_map[src_ep]
            submit_overhead = 0.5
            transfer_task_id, expected_end_time = self.mock_data_manager.submit_transfer_task(src_ep, dest_ep, transfer_size, self.current_time + timedelta(seconds=submit_overhead), self.mock_ds.transferring_size, self.mock_ds.transferring_file_num)
            self.mock_transfer_histroy[transfer_task_id] = task_record['id']
        
            self.schedule_event(Event(EventType.TRANSFER_COMPLETE, self.current_time, expected_end_time, {"transfer_id": transfer_task_id, "src_ep": src_ep, "dest_ep": dest_ep}))
            transfer_cnt += 1
        return transfer_cnt

    def launch_task(self, task_record):
        ep = task_record["simu_executor"]
        submit_overhead = 0.1
        expected_end_time = self.mock_endpoints_copy[ep].submit_task(task_record, self.current_time + timedelta(seconds=submit_overhead))
        self.mock_task_status_tbl[task_record['id']] = MockTaskStates.running
        task_record["added_to_analyzer"] = True
        self.schedule_event(Event(EventType.TASK_COMPLETE, self.current_time, expected_end_time, {"execution_task_record": task_record}))




    def schedule_event(self, event: Event):
        """将事件加入优先队列"""
        heapq.heappush(self.event_queue, event)


    def process_next_event(self) -> bool:
        """处理下一个事件，返回是否还有更多事件"""
        if not self.event_queue:
            return False

        event = heapq.heappop(self.event_queue)
        self.current_time = event.end_time

        if event.type == EventType.TASK_COMPLETE:
            self.handle_task_completion(event)
        elif event.type == EventType.TRANSFER_COMPLETE:
            self.handle_transfer_completion(event)

        return True

    def simulate(self):
        time.sleep(70)
        
        start_time = datetime.now()
        self.initialize_events(start_time)
        
        # 主事件循环
        while True:
            is_next = self.process_next_event()
            if not is_next:
                break
        print(f"makespan {(self.current_time - start_time).total_seconds()}")
        return

    def handle_task_completion(self, event: Event):
        payload = event.payload
        task_record = payload["execution_task_record"]
        task_id = task_record['id']
        executor = task_record['simu_executor']

        self.mock_endpoints_copy[executor].pop_task(task_record, self.current_time)
        self.mock_task_status_tbl[task_id] = MockTaskStates.completed

        for child in graphHelper.workflow_graph[task_id]:
            if self.check_task_ready_for_data_transfer(child):
                child_record = graphHelper.id_to_task[child]
                self.do_schedule_for_task(child_record)

    

    def handle_transfer_completion(self, event: Event):
        transfer_task_id = event.payload["transfer_id"]
        src_ep  = event.payload["src_ep"]
        dest_ep = event.payload["dest_ep"]

        self.mock_data_manager.pop_task(src_ep, dest_ep, transfer_task_id)

        related_task_id = self.mock_transfer_histroy[transfer_task_id]
        self.mock_transfer_counter[related_task_id] -= 1
        # 需要pop transfer task


        if self.mock_transfer_counter[related_task_id] == 0:
            task_record = graphHelper.id_to_task[related_task_id]
            self.launch_task(task_record)
            del self.mock_transfer_counter[related_task_id]      


    def add_mock_endpoint(self, executor_label, host_ip, current_cores, min_blocks, max_blocks, workers_per_block, max_workers):
        if self.enabled:
            if executor_label not in self.mock_endpoints:
                self.mock_endpoints[executor_label] = MockEndpoint(executor_label,host_ip, current_cores, 
                min_blocks, max_blocks, workers_per_block, max_workers)

    def add_data_manager(self, manager):
        self.data_manager = manager

    def start_simulation(self):
        if not self.enabled:
            return
        if self.simulation_thread is None or not self.simulation_thread.is_alive():
            self.simulation_thread = threading.Thread(target=self.simulate)
            self.simulation_thread.daemon = True
            self.simulation_thread.start()


    def check_task_ready_for_data_transfer(self, task_id):
        # TODO: 这里有个坑，遇到compression任务可能会崩溃
        task_record = graphHelper.id_to_task[task_id]
        dep_id_list = [ dep.task_def["id"] for dep in task_record["depends"]]
        return all(self.mock_task_status_tbl[dep_id] == MockTaskStates.completed 
              for dep_id in dep_id_list)


    def mock_DHEFT_schedule(self, task_record, transfer_map):
        transfer_cost = 0
        eft_dict = {}
        for ep in self.mock_endpoints:
            eft_dict[
                ep
            ] = self.mock_ds.calculate_transfer_cost(
                transfer_map, ep, self.data_manager.bandwidth_info
            )
            eft_dict[ep] += self.mock_ds.calculate_ideal_execution_duration(
                ep, task_record, self.mock_endpoints[ep].current_cores
            )
        return min(eft_dict, key=eft_dict.get)


    def prepare_mock_transfer(self, task_record):  
        output_map = {}
        data_trans_list = []

        args = task_record["args"]
        kwargs = task_record["kwargs"]
        data_to_trans = TransferPredictor.check_data_transfer(
            args
        ) + TransferPredictor.check_data_transfer(kwargs)

        for data in data_to_trans:
            tmp_res = self.verify_endpoint_for_remotefile(data)
            if tmp_res[0] is not None and tmp_res[0] not in output_map:
                output_map[tmp_res[0]] = 0
            if tmp_res[0] is not None :
                output_map[tmp_res[0]] += tmp_res[1]
        
        unfinished_future = MockAnalyzer.check_unfinished_future(args) + MockAnalyzer.check_unfinished_future(kwargs)
        for unf_fu in unfinished_future:
            tmp_task_record = unf_fu.task_def
            if "simu_executor" in tmp_task_record:
                simu_executor = tmp_task_record["simu_executor"]
                if simu_executor in output_map:
                    output_map[simu_executor] += tmp_task_record["predict_output"]
                else:
                    output_map[simu_executor] = tmp_task_record["predict_output"]
        return output_map

    
    def verify_endpoint_for_remotefile(self, data):
        if isinstance(data, RemoteFile) or isinstance(data, RemoteDirectory):
            rsync_ip = data.rsync_ip
            src_ep = None
            for ep in self.mock_endpoints:
                if rsync_ip == self.mock_endpoints[ep].host_ip:
                    src_ep = ep
            return (src_ep, data.file_size)
        else:
            return (None, 0)