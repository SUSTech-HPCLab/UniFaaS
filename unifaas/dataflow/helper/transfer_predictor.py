import logging
import os
from unifaas.dataflow.helper.execution_recorder import UNIFAAS_HOME
from queue import PriorityQueue
from unifaas.dataflow.helper.resource_status_poller import ResourceStatusPoller
from unifaas.executors.funcx.executor import FuncXExecutor
from funcx.sdk.file import RemoteFile, RemoteDirectory
import time
import re
logger = logging.getLogger("unifaas")
from concurrent.futures import Future

class TransferPredictor:
    """TransferPredictor is a class that predicts the transfer time of a task
        - Build a model to estimate bandwidth of any two endpoints
        - Calculate the transfer time of a task 
    """
    __instance = None
    __first_init = False

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = object.__new__(cls)
        return cls.__instance

    def __init__(self, executors, bandwith_info=None, dtc=None,):
        if TransferPredictor.__first_init == False:
            self.executors = executors
            self.network_info = {}
            self.probe_dic = {}
            self.dtc = dtc
            self._init_bandwith_info(bandwith_info)
            self.resource_poller = ResourceStatusPoller(self.executors)
            self.executor_address_info = {}
            self._gather_endpoints_address_info()
            TransferPredictor.__first_init = True


    def _gather_endpoints_address_info(self):
            """
                gather the information of address.
                RsyncClient gather, ip,username, data_pathls
                GlobusClient gather the globus_id, data_path
            """
            for exe_key in self.executors.keys():
                #all info contains the idle workers/address info etc
                funcx_executor = self.executors[exe_key]
                if isinstance(funcx_executor, FuncXExecutor):
                    info = self.resource_poller.get_resource_status_by_label(exe_key)
                    data_path = info.get('local_data_path')
                    rsync_username = info.get('rsync_username')
                    globus_ep_id = info.get('globus_ep_id')
                    host_ip = info.get('rsync_ip') 
                    self.executor_address_info[exe_key] = {
                        "identifier": f"{rsync_username}@{host_ip}",
                        "data_path" :data_path,
                        "username" : rsync_username,
                        "host_ip" : host_ip,
                        "globus_ep_id" : globus_ep_id
                    }
        

    def _init_bandwith_info(self,bandwith_info=None):
        """Init the network bandwidth info between any two endpoints"""
        if bandwith_info is None:
            # firstly, create dummy file to get the bandwidth
            funcx_executor =[]
            gengerated_file = []
            for exe_key_from in self.executors.keys():
                executor_from = self.executors[exe_key_from]
                if isinstance(executor_from, FuncXExecutor):
                    funcx_executor.append(exe_key_from)
                    fu = executor_from.handle_probe_file()
                    self.probe_dic[exe_key_from] = fu

            # secondly, send and record
            transfer_tasks_for_prob = []
            for fx_exe in funcx_executor:
                prob_list = self.probe_dic[fx_exe].result() #result is a dict, with execution info
                if isinstance(prob_list, dict):
                    prob_list = prob_list['result'] # prob_list is a list of remote file
                for dest_exe in funcx_executor:
                    for prob_file in prob_list:
                        gengerated_file.append(prob_file.file_name)
                        transfer_task = self.dtc.transfer(prob_file, dest_exe)
                        transfer_tasks_for_prob.append(transfer_task['task_id'])
            
            # thirdly, wait for all transfer tasks to finish
            while len(transfer_tasks_for_prob) > 0:
                for task_id in transfer_tasks_for_prob:
                    if self.dtc.transfer_tasks[task_id]['status'] == 'SUCCEEDED':
                        transfer_tasks_for_prob.remove(task_id)
                time.sleep(1)
            
            raise Exception("Not support empty bandwith information")
        

        for exe_key_from in bandwith_info.keys():
            self.network_info[exe_key_from] = {}
            for exe_key_to in bandwith_info.keys():
                if exe_key_from != exe_key_to:
                    self.network_info[exe_key_from][exe_key_to]={
                            'bandwidth': bandwith_info[exe_key_from][exe_key_to],
                            'transfer_queue': PriorityQueue(),                                   
                        }
                        
    
    def perdict_based_on_bandwith(self,from_exe, to_exe, transfer_size,num_workers=9, latency=1):
        """
            num_workers: the number of workers
            bandwith: the bandwith of the network
            file_size: the size of the file
        """
        if from_exe == to_exe:
            return 0
        bandwith = self.network_info[from_exe][to_exe]['bandwidth']
        bandwith_to_bytes = bandwith * 1024 * 1024
        pure_trans_time = transfer_size / (bandwith_to_bytes / num_workers)
        return pure_trans_time + latency

    def predict_est_for_certain_executor(self, task_record, dest_executor):
        # predict the earliest start time for each task_record

        # Firstly, calculate the ready remotefile in args
        args = task_record['args']
        kwargs = task_record['kwargs']
        # list concat
        data_to_trans = TransferPredictor.check_data_transfer(args) + \
        TransferPredictor.check_data_transfer(kwargs)
        cur_time = time.time()
        est_list = []
        net_latency = 1 # 1 second for start transfer
        for data in data_to_trans:
            rsync_ip = data.rsync_ip
            resource = self.resource_poller.get_resource_status()
            start_executor = None
            for key in resource.keys():
                if resource[key]['rsync_ip'] == rsync_ip:
                    start_executor = key
            if start_executor is None:
                start_executor = "data_pool" # the remote file not belong to any executor/ep
            est_list.append(cur_time+ net_latency + self.perdict_based_on_bandwith(start_executor, dest_executor, data.file_size))
        
        # Secondly, calculate the dependencies' eft
        for dep in task_record['depends']:
            dep_record = dep.task_def
            start_executor = dep_record['executor']
            data_size = dep_record['predict_output']
            if 'predict_eft' in dep_record.keys() and not dep.done():
                predict_eft = dep_record['predict_eft']
            else:
                predict_eft = cur_time
            est_list.append(predict_eft + net_latency + self.perdict_based_on_bandwith(start_executor, dest_executor, data_size))
        
        if len(est_list) == 0:
            return 0

        return max(est_list)

    def predict_comm_cost(self, task_i, ep_m, ep_n):
        transfer_size = task_i['predict_output']
        return self.perdict_based_on_bandwith(ep_m, ep_n, transfer_size)

    def predict_avg_comm_cost(self, input_size):
        if len(self.network_info.keys()) <= 1:
            return 0
        transfer_size = input_size
        res = 0
        for ep_i in self.network_info.keys():
            for ep_j in self.network_info.keys():
                if ep_i != ep_j:
                    res +=self.perdict_based_on_bandwith(ep_i, ep_j, transfer_size)
        return res / (len(self.network_info.keys()) * (len(self.network_info.keys()) - 1))


    def real_time_predict_for_transfer(self, transfer_size, src_ep, dest_ep,num_workers=3):
        cur_transffering_size = self.dtc.get_transferring_size(src_ep, dest_ep)
        cur_transffering_num = self.dtc.get_transferring_file_num(src_ep, dest_ep)
        if src_ep == dest_ep:
            return 0
        bandwith = self.network_info[src_ep][dest_ep]['bandwidth']
        bandwith_to_bytes = bandwith * 1024 * 1024
        pure_trans_time = cur_transffering_size / bandwith_to_bytes + 0.1*cur_transffering_num + transfer_size / (bandwith_to_bytes / num_workers)
        return pure_trans_time


    def real_time_predict_comm_cost_for_task_record(self, task_record, ep):
        # task: task record, contains the dependencies
        # ep: the executor to run the task
        # This function should calculate the earliest finish time for each transfer of dependency
        args = task_record['args']
        kwargs = task_record['kwargs']
        # list concat
        data_to_trans = TransferPredictor.check_data_transfer(args) + \
        TransferPredictor.check_data_transfer(kwargs)
        transfer_size_cnt = {}
        for data in data_to_trans:
            rsync_ip = data.rsync_ip
            resource = self.resource_poller.get_resource_status()
            start_executor = None
            for key in resource.keys():
                if resource[key]['rsync_ip'] == rsync_ip:
                    start_executor = key
            if start_executor is None:
                start_executor = "data_pool" # the remote file not belong to any executor/ep
            if start_executor not in transfer_size_cnt.keys():
                transfer_size_cnt[start_executor] = 0
            transfer_size_cnt[start_executor] += data.file_size
        transfer_cost = 0
        for src_ep in transfer_size_cnt.keys():
            if transfer_size_cnt[src_ep] > 0:
                transfer_cost = max(transfer_cost, self.real_time_predict_for_transfer(transfer_size_cnt[src_ep], src_ep, ep) )
        
        return transfer_cost

    @staticmethod
    def parse_endpoint(endpoint_with_core_id):
        x = re.search("core\d+$", endpoint_with_core_id)
        if x is None:
            return endpoint_with_core_id
        else:
            return endpoint_with_core_id[:x.start()-1]

    def find_target_ep(self,file):
        src_ip = file.rsync_ip
        src_username = file.rsync_username
        identifier = f"{src_username}@{src_ip}"
        for exe in self.executor_address_info:
            if identifier == self.executor_address_info[exe]['identifier']:
                return exe
        return None


    def predict_init_comm_cost(self, task_record, ep):
        start_ep = ep
        args = task_record['args']
        kwargs = task_record['kwargs']
        data_to_trans = TransferPredictor.check_data_transfer(args) + \
        TransferPredictor.check_data_transfer(kwargs)

        if len(data_to_trans) == 0:
            return 0

        data_transfer_times = [ self.perdict_based_on_bandwith(self.find_target_ep(data), ep , data.file_size) for data in data_to_trans]
        
        return max(data_transfer_times)

    def caculate_transfer_size(self, task_record, ep):
        start_ep = ep
        args = task_record['args']
        kwargs = task_record['kwargs']
        data_to_trans = TransferPredictor.check_data_transfer(args) + \
        TransferPredictor.check_data_transfer(kwargs)
        sum_size = 0

        for data in data_to_trans:
            if self.find_target_ep(data) != ep:
                sum_size += data.file_size
        return sum_size

    
    
    @staticmethod
    def check_data_transfer(res):
        data_trans_list = []
        try:

            if isinstance(res, Future) and res.done():
                res = res.result()


            if isinstance(res, RemoteFile) or isinstance(res, RemoteDirectory):
                    data_trans_list.append(res)

            if isinstance(res, tuple):
                for item in res:
                    data_trans_list += TransferPredictor.check_data_transfer(item)

            if isinstance(res, list):
                for item in res:
                    data_trans_list += TransferPredictor.check_data_transfer(item)

            if isinstance(res, dict):
                for val in res.values():
                    data_trans_list += TransferPredictor.check_data_transfer(val)
        except Exception as e:
            return data_trans_list

        return data_trans_list