from dataclasses import dataclass
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from collections import deque
import heapq

class TaskNode:

    def __init__(self, task_id, start_time, expected_duration, end_time):
        self.task_id = task_id
        self.next = None
        self.prev = None
        self.start_time = start_time
        self.expected_duration = expected_duration
        self.end_time = end_time

class LinkedListWithIndex:
    def __init__(self):
        self.dummy_head = TaskNode(-1, None, None, None)
        self.dummy_end = TaskNode(-1, None, None, None)
        self.dummy_head.next = self.dummy_end
        self.dummy_end.prev = self.dummy_head
        self.index = {}

    def append_at_end(self, task_id, start_time, expected_duration, end_time):
        new_node = TaskNode(task_id, start_time, expected_duration, end_time)
        new_node.prev = self.dummy_end.prev
        new_node.next = self.dummy_end
        self.dummy_end.prev.next = new_node
        self.dummy_end.prev = new_node
        self.index[task_id] = new_node

    def remove(self, task_id):
        if task_id not in self.index:
            return
        node_to_remove = self.index[task_id]
        node_to_remove.prev.next = node_to_remove.next
        node_to_remove.next.prev = node_to_remove.prev
        del self.index[task_id]

class MockCore:

    def __init__(self, host_endpoint, core_id):
        self.host_endpoint = host_endpoint
        self.core_id = core_id
        self.task_list = LinkedListWithIndex()
        self.earliest_available_time = datetime.now()
        self.expire_time = None # 默认销毁时间为None，当scale in时需要对mock core标记expire time
        

    def __lt__(self, other):
        # 定义小于运算符，以便heapq能根据earliest_available_time进行排序
        if self.earliest_available_time  ==  other.earliest_available_time:
            return self.core_id < other.core_id
        else:
            return self.earliest_available_time < other.earliest_available_time

    def push_task(self, task_record, simulation_time=None):
        if not simulation_time:
            tmp_timestamp = datetime.now()
        else:
            tmp_timestamp = simulation_time

        task_id = task_record['id']
        if 'predict_execution' in task_record and self.host_endpoint in task_record['predict_execution']:
            execution_time = task_record['predict_execution'][self.host_endpoint]
            # TODO: 这里应该会造成bug
            if 'transfer_cost_for_analyzer' in task_record:
                start_time =  max(tmp_timestamp +   timedelta(seconds=task_record['transfer_cost_for_analyzer']), self.earliest_available_time)
            else:
                start_time = max(tmp_timestamp , self.earliest_available_time)

            expected_duration = execution_time + 1
            end_time = start_time + timedelta(seconds=expected_duration)
            self.task_list.append_at_end(task_id, start_time, expected_duration, end_time)

              # 更新最早可用时间
            if self.earliest_available_time is None or end_time > self.earliest_available_time:
                self.earliest_available_time = end_time
            return end_time
            #print(f"push task {task_id} with start_time {start_time} and end_time {end_time} and  core's eat {self.earliest_available_time}")
        return None
        
    
    def pop_task(self, task_record, simulation_time=None): 
        if not simulation_time:
            tmp_timestamp = datetime.now()
        else:
            tmp_timestamp = simulation_time

        self.task_list.remove(task_record['id'])
        original_eat = self.earliest_available_time

        if len(self.task_list.index) == 0:
            self.earliest_available_time = tmp_timestamp
        else:
            self.earliest_available_time = max(node.end_time for node in self.task_list.index.values())

        if original_eat != self.earliest_available_time:
            return True
        else:
            return False





class MockEndpoint:
    def __init__(self, executor_label, host_ip, current_cores, min_blocks, max_blocks, workers_per_block, max_workers):
        self.executor_label = executor_label
        self.host_ip = host_ip
        self.current_cores = current_cores
        self.min_blocks = min_blocks
        self.max_blocks = max_blocks
        self.workers_per_block = workers_per_block
        self.max_workers = max_workers
        self.total_tasks = 0
        # 需要一个task id to cores , 快速清除任务
        self.simulation_runtime = []  # 初始化为一个空的列表，用于小根堆
        self.core_map = {}
        self.task_to_core = {}
        self._init_core_status()
        self._eat_change_counter = 0  # eat: earliest avaliable time 


    def __hash__(self):
        return hash(self.executor_label)

    def __eq__(self, other):
        if isinstance(other, MockEndpoint):
            return self.executor_label == other.executor_label
        return False

    def _init_core_status(self):
        for core_id in range(self.current_cores):
            core = MockCore(self.executor_label, core_id)
            self.core_map[core_id] = core
            heapq.heappush(self.simulation_runtime, core)

    def submit_task(self, task_record, simulation_time=None):
        # 先找出最早available的核，然后将任务加入到最小核的task_list中
        if self._eat_change_counter > self.current_cores * 0.2:
            self._reheapify()

        target_core = heapq.heappop(self.simulation_runtime)
        end_time = target_core.push_task(task_record, simulation_time)
        self.task_to_core[task_record['id']] = target_core
        heapq.heappush(self.simulation_runtime, target_core)
        self.total_tasks += 1
        return end_time

    def pop_task(self, task_record, simulation_time=None):
        target_core = self.task_to_core[task_record['id']] 
        if target_core:
            is_modify_eat = target_core.pop_task(task_record, simulation_time)
            if is_modify_eat:
                self._eat_change_counter += 1
        del self.task_to_core[task_record['id']] 
        self.total_tasks -= 1
        #print(f"pop task {task_record['id']} with at simulation_time {simulation_time}")



    def _reheapify(self):
        heapq.heapify(self.simulation_runtime)
        self._eat_change_counter = 0
