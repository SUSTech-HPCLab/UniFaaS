import logging
from unifaas.dataflow.analyzer.mock_endpoint import MockEndpoint
import copy
from unifaas.dataflow.helper.graph_helper import graphHelper
from unifaas.dataflow.states import States
import threading
import time
from unifaas.dataflow.analyzer.mock_data_manager import MockDataManager
from unifaas.dataflow.data_transfer_management import DataTransferManager
from unifaas.dataflow.helper.resource_status_poller import ResourceStatusPoller
from datetime import datetime, timedelta
from queue import PriorityQueue
from funcx.sdk.file import RemoteFile, RemoteDirectory
from unifaas.dataflow.helper.transfer_predictor import TransferPredictor
from concurrent.futures import Future
simulation_logger = logging.getLogger("simulation")

from enum import IntEnum


class MockTaskStates(IntEnum):
    not_scheduled = 0
    staging = 2
    running = 3
    completed = 4

class MockTaskWithPriority:
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


class MockSchedulerDataSource:

    def __init__(self, data_manager, resource_poller):
        self.data_manager = data_manager
        self.resource_poller = resource_poller

        self.cur_being_executed_time = copy.deepcopy(resource_poller.cur_being_executed_time)
        self.transferring_size = copy.deepcopy(self.data_manager.dtc.transferring_size)
        self.transferring_file_num = copy.deepcopy(self.data_manager.dtc.transferring_file_num)

    def calculate_ideal_execution_duration(self, executor, task_record, total_workers):
        if total_workers == 0:
            return float("inf")
        return (
            self.cur_being_executed_time[executor]
            / total_workers
            + task_record["predict_execution"][executor]
        )
    
    def calculate_transfer_cost(self, transfer_map, dest_ep, bandwidth_map):
        transfer_cost = 0
        for src_ep in transfer_map:
            if src_ep == dest_ep:
                continue
            if transfer_map[src_ep] > 0:
                bandwidth = bandwidth_map[src_ep][dest_ep]
                transfer_cost = max(
                    transfer_cost,
                    self.predict_for_each_transfer(
                        transfer_map[src_ep], src_ep, dest_ep, bandwidth
                    ),
                )
        return transfer_cost
    
    def predict_for_each_transfer(self, transfer_size, src_ep, dest_ep, bandwidth):
        if src_ep == dest_ep:
            return 0
        cur_transffering_size = self.transferring_size[src_ep][dest_ep]
        cur_transffering_num = self.transferring_file_num[src_ep][dest_ep]
        num_workers = 3 

        bandwidth_to_bytes = bandwidth * 1024 * 1024
        pure_trans_time = (
            cur_transffering_size / bandwidth_to_bytes
            + 0.1 * cur_transffering_num
            + transfer_size / (bandwidth_to_bytes / num_workers)
        )
        return pure_trans_time





class MockAnalyzer:


    @staticmethod
    def check_unfinished_future(res):
        unfinished_future = []
        try:
            if isinstance(res, Future) and not res.done():
                unfinished_future.append(res)

            if isinstance(res, tuple):
                for item in res:
                    unfinished_future += MockAnalyzer.check_unfinished_future(item)

            if isinstance(res, list):
                for item in res:
                    unfinished_future +=  MockAnalyzer.check_unfinished_future(item)
            if isinstance(res, dict):
                for val in res.values():
                    unfinished_future +=  MockAnalyzer.check_unfinished_future(item)
        except Exception as e:
            return unfinished_future

        return unfinished_future


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
  
    def __init__(self, enabled):
        self.enabled = enabled
        self.mock_endpoints = {}
        self.mock_data_manager = MockDataManager()
        self.simulation_thread = None
        #self.start_simulation()

    def add_data_manager(self, manager):
        self.data_manager = manager


    def add_mock_endpoint(self, executor_label, host_ip, current_cores, min_blocks, max_blocks, workers_per_block, max_workers):
        if self.enabled:
            if executor_label not in self.mock_endpoints:
                self.mock_endpoints[executor_label] = MockEndpoint(executor_label,host_ip, current_cores, 
                min_blocks, max_blocks, workers_per_block, max_workers)


    def submit_task_record_to_mock_endpoint(self, task_record, target_ep, mock_endpoints_copy=None, simulation_time=None):
        # TODO: 此处需要查看是否是compression任务？
        # TODO: 此处应该去mock manager里调度数据传输
        if self.enabled:
            if mock_endpoints_copy:
                mock_endpoints_copy[target_ep].submit_task(task_record, simulation_time)
            else:
                self.mock_endpoints[target_ep].submit_task(task_record)
            task_record["added_to_analyzer"] = True

    def remove_task_record_from_mock_endpoint(self, task_record, target_ep, mock_endpoints_copy=None):
        if self.enabled:
            if mock_endpoints_copy:
                mock_endpoints_copy[target_ep].pop_task(task_record)
            else:
                self.mock_endpoints[target_ep].pop_task(task_record)


    def deepcopy_for_simulation(self):
        # TODO: 需要计算一下deepcopy的资源消耗
       
        self.running_tasks_set = set() # 只在开启模拟时使用
        self.unscheduled_tasks_set = set() # 只在开启模拟时使用
        self.mock_transfer_counter = {}
        self.mock_endpoints_copy = copy.deepcopy(self.mock_endpoints)
        self.mock_task_status_tbl = {}
        self.mock_transfer_histroy = {} # transfer_id to task_id
        # task_id 的拓扑使用 graph.workflow_graph
        # 遍历raw_graph 查看task record的状态
        for task_app in graphHelper.raw_graph:
            task_record = task_app.task_def
            task_id = task_record['id']
            if task_record['status'] == States.exec_done:
                self.mock_task_status_tbl[task_record['id']] = MockTaskStates.completed
                task_record['simu_executor'] = task_record['executor']
            elif 'added_to_analyzer' in task_record:
                self.mock_task_status_tbl[task_record['id']] = MockTaskStates.running
                self.running_tasks_set.add((task_id, task_record['executor']))
                task_record['simu_executor'] = task_record['executor']
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


       

    def update_task_completion(self, simulation_time):
        completed_tasks = []

        for task_id, ep_label in self.running_tasks_set:
            mock_ep = self.mock_endpoints_copy[ep_label]
            core = mock_ep.task_to_core.get(task_id)
            if core:
                task_node = core.task_list.index.get(task_id)
                if task_node and task_node.end_time <= simulation_time:
                    completed_tasks.append((task_id, ep_label))

        # 处理完成的任务
        for task_id, ep_label in completed_tasks:
            self.mock_endpoints_copy[ep_label].pop_task({'id': task_id}, simulation_time)
            self.mock_task_status_tbl[task_id] = MockTaskStates.completed
            task_record = graphHelper.id_to_task[task_id]
            self.mock_ds.cur_being_executed_time[task_record["simu_executor"]] -= task_record["predict_execution"][
                    task_record["simu_executor"]
            ]
            self.running_tasks_set.remove((task_id, ep_label))  # 从running set中移除
        



    def check_task_ready_for_data_transfer(self, task_id):
        task_record = graphHelper.id_to_task[task_id]
        dep_id_list = [ dep.task_def["id"] for dep in task_record["depends"]]
        return all(self.mock_task_status_tbl[dep_id] == MockTaskStates.completed 
              for dep_id in dep_id_list)


    def simulate_workflow_process(self):

        simulation_time = datetime.now()  # 模拟起始时间
        TIME_STEP = timedelta(seconds=1)      # 时间步长
        MAX_SIMULATION_TIME = timedelta(seconds=1000)  # 最大模拟时间

        end_time = simulation_time + MAX_SIMULATION_TIME

        #TODO: 在这里模拟器的逻辑不与实际调度算法混淆，调度算法使用的依据是 cur_transferring_file_size 和 cur_running_time
        #TODO: 模拟器只模拟实际的执行情况，不影响调度算法的调度结果
        iter_times = 0
        schedule_result = {}
        execution_result = {}
        while simulation_time < end_time:             
            # 1. 更新任务完成状态
            if len(self.unscheduled_tasks_set) == 0 and len(self.running_tasks_set) == 0  and len(self.mock_transfer_histroy) == 0:
                simulation_logger.info("SimulationEnd")
                for func in schedule_result:
                    for ep in schedule_result[func]:
                        simulation_logger.info(f"{func} {ep} : {schedule_result[func][ep]}  {execution_result[func][ep]['time'] / execution_result[func][ep]['num']}")
                break

            simulation_logger.info("---")
            simulation_logger.info(f"SimulationTime: {simulation_time}")
            simulation_logger.info(f"TaskStatus: unschedule:{len(self.unscheduled_tasks_set)}|running:{len(self.running_tasks_set)}|transferring{len(self.mock_transfer_histroy)}")

            info_line = ""
            active_worker_info_line = ""
            schedule_info = ""
            for ep in self.mock_endpoints_copy:
                info_line += f"{ep}:{self.mock_endpoints_copy[ep].total_tasks}|"
                active_workers = min(self.mock_endpoints_copy[ep].current_cores, self.mock_endpoints_copy[ep].total_tasks)
                active_worker_info_line += f"{ep}:{active_workers}|"
            
            simulation_logger.info(f"EndpointStatus: {info_line}")
            simulation_logger.info(f"ActiveWorker: {active_worker_info_line}")


            self.update_task_completion(simulation_time)

            tasks_to_schedule = PriorityQueue()  
            tasks_to_remove = set()
            


            for task_id in self.unscheduled_tasks_set:
                if self.check_task_ready_for_data_transfer(task_id):
                    tasks_to_schedule.put(MockTaskWithPriority(graphHelper.id_to_task[task_id]))
                    tasks_to_remove.add(task_id)

            self.unscheduled_tasks_set -= tasks_to_remove

            while not tasks_to_schedule.empty():
                mock_prio_task = tasks_to_schedule.get()
                # TODO: mock_DHEFT_schedule 需要完善
                task_record = mock_prio_task.task_record
                mock_to_transfer_map = self.prepare_mock_transfer(task_record)

                task_record["simu_executor"] = self.mock_DHEFT_schedule(task_record, mock_to_transfer_map)
                
                if task_record["func_name"] not in schedule_result:
                    schedule_result[task_record["func_name"]] = {}
                    execution_result[task_record["func_name"]] = {}
                if task_record["simu_executor"] not in schedule_result[task_record["func_name"]]:
                    schedule_result[task_record["func_name"]][task_record["simu_executor"]] = 0
                    execution_result[task_record["func_name"]][task_record["simu_executor"]] = {
                        "num" : 0,
                        "time" : 0,
                    }

                schedule_result[task_record["func_name"]][task_record["simu_executor"]] += 1
                execution_result[task_record["func_name"]][task_record["simu_executor"]]["num"] += 1
                execution_result[task_record["func_name"]][task_record["simu_executor"]]["time"] += task_record["predict_execution"][
                    task_record["simu_executor"]
                ]
        
                # 发送给data manager
                mock_transfer_tasks = []
                for src_ep in mock_to_transfer_map:
                    if src_ep != task_record["simu_executor"]:
                        mock_transfer_tasks.append((src_ep,mock_to_transfer_map[src_ep]))
                
                for transfer_task in mock_transfer_tasks:
                    transfer_task_id, expected_end_time  = self.mock_data_manager.submit_transfer_task(transfer_task[0], task_record["simu_executor"],transfer_task[1], simulation_time, self.mock_ds.transferring_size, self.mock_ds.transferring_file_num)
                    self.mock_transfer_histroy[transfer_task_id] = task_record['id']

                self.mock_ds.cur_being_executed_time[task_record["simu_executor"]] += task_record["predict_execution"][
                    task_record["simu_executor"]
                ]
                 
                self.mock_transfer_counter[task_record["id"]] = len(mock_transfer_tasks)
                self.mock_task_status_tbl[task_record["id"]] = MockTaskStates.staging

            # 查询data manager 完成了哪些任务
            completed_transfers = self.mock_data_manager.update_all_transfers(simulation_time, self.mock_ds.transferring_size, self.mock_ds.transferring_file_num)
            for t in completed_transfers:
                if t in self.mock_transfer_histroy:
                    task_id = self.mock_transfer_histroy[t]
                    self.mock_transfer_counter[task_id] -= 1
                    del self.mock_transfer_histroy[t]

            tasks_to_launch = PriorityQueue() 
            keys_to_delete = []
            for task_id in self.mock_transfer_counter:
                if self.mock_transfer_counter[task_id] == 0:
                    tasks_to_launch.put(MockTaskWithPriority(graphHelper.id_to_task[task_id]))
                    keys_to_delete.append(task_id)
            
            for key in keys_to_delete:
                del self.mock_transfer_counter[key]

            while not tasks_to_launch.empty():
                mock_prio_task = tasks_to_launch.get()
                task_record = mock_prio_task.task_record
                self.mock_task_status_tbl[task_record['id']] = MockTaskStates.running
                self.submit_task_record_to_mock_endpoint(task_record, task_record["simu_executor"], mock_endpoints_copy=self.mock_endpoints_copy, simulation_time=simulation_time)
                self.running_tasks_set.add((task_record["id"], task_record["simu_executor"]))

            simulation_time += TIME_STEP
            iter_times += 1
        


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

            
        
    
            
    def start_simulation(self):
        if not self.enabled:
            return
        if self.simulation_thread is None or not self.simulation_thread.is_alive():
            self.simulation_thread = threading.Thread(target=self.do_simulation)
            self.simulation_thread.daemon = True
            self.simulation_thread.start()


    def do_simulation(self):
        start_time = time.time()
        while True:
            if time.time() - start_time < 20:
                time.sleep(5)
            else:
                # self.deepcopy_for_simulation()
                # self.simulate_workflow_process()
                break

    

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

    

        

