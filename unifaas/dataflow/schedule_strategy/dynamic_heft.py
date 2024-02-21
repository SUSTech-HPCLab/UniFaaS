from unifaas.dataflow.states import States
import numpy as np
import heapq
from queue import Queue
import time
from unifaas.dataflow.helper.graph_helper import GraphHelper
from functools import partial
import logging
exp_logger = logging.getLogger("experiment")

class DynamicHEFTScheduling():

    def __init__(self,execution_predictor, transfer_predictor):
        self.init_resource()
        self.predictor = execution_predictor
        self.transfer_predictor = transfer_predictor

        
    
    def init_resource(self):
        self.id_to_task = {}
        self.fu_to_task = {}
        self.id_dag = {}
        self.source_nodes = []
        self.pure_dag = {}
        self.indegree_dict = {}
        self.func_dict = {}
        self.rank_memory = {} # {task_id:rank}
        self.prediction_result = {} # {endpoint : {total_time, total_task_num}}

    def avg_comm_cost(self,task_i):
        task_record_i = self.id_to_task[task_i]
        if 'predict_output' not in task_record_i.keys():
            (func_name, input_size, predict_execution, predict_output) = self.predictor.predict(task_record_i)
        input_size = task_record_i['input_size']
        return self.transfer_predictor.predict_avg_comm_cost(input_size)


    def ranku(self, task_id):
        rank_res = self.rank_memory.get(task_id, None)
        if rank_res is not None:
            return rank_res

        succ_factor = 0
        comm_cost = self.avg_comm_cost(task_id)
        task_record = self.id_to_task[task_id]    
        avg_predict_execution = sum(task_record['predict_execution'].values())/len(task_record['predict_execution'])
        for key in task_record['predict_execution'].keys():
            if key not in self.prediction_result.keys():
                self.prediction_result[key] = {"total_time":0, "total_task_num":0}
            self.prediction_result[key]['total_time'] += task_record['predict_execution'][key]
            self.prediction_result[key]['total_task_num'] += 1


        for v in self.id_dag[task_id]:
            succ_factor = max(comm_cost + self.ranku(v), succ_factor)
            #succ_factor = max(self.ranku(v), succ_factor)

        self.rank_memory[task_id] = succ_factor + avg_predict_execution
        self.id_to_task[task_id]['heft_priority'] = self.rank_memory[task_id]
        
        return self.rank_memory[task_id]

    
    def update_resource(self, graphHelper):
        self.init_resource()
        self.id_dag, self.source_nodes, self.id_to_task, self.fu_to_task,\
            self.indegree_dict  = GraphHelper.generate_dag_info(graphHelper)

    def check_sufficiency_of_info(self):
        # Check if the predictor is available
        if self.predictor is None:
            return False

        for node in self.id_dag.keys():
            node_task = self.id_to_task[node]
            if node_task['func_name'] not in self.func_dict :
                self.func_dict[node_task['func_name']] = {"task_num":0, "completed_num":0}
            self.func_dict[node_task['func_name']]['task_num'] += 1
        # Check the integrity of workflow information
        hit_history, out_history = self.predictor.look_up_history_model(self.func_dict.keys())
        if len(out_history) == 0:
            return True
        else:
            return False

    def dynamic_heft(self):
        task_set = set()
        q = Queue()
        for node in self.source_nodes:
            q.put(node)
        while not q.empty():
            node = q.get()
            task_set.add(node)
            for child in self.id_dag[node]:
                q.put(child)
        task_list = list(task_set)
        rank_list = [self.ranku(task_id) for task_id in task_list]
        sorted_pairs = sorted(zip(task_list, rank_list), key=lambda x: -x[1])
        sorted_task_list = [x[0] for x in sorted_pairs]
        return sorted_task_list

    def calculate_performance_ratio(self):
        ratio_result = {}
        max_avg = 0
        for key in self.prediction_result.keys():
            ratio_result[key] = self.prediction_result[key]['total_time']/self.prediction_result[key]['total_task_num']
            max_avg = max(max_avg, ratio_result[key])
        
        for key in ratio_result.keys():
            ratio_result[key] = max_avg/ratio_result[key] if ratio_result[key] != 0 else 1

        return ratio_result
        


def dynamic_heft_scheduling(graphHelper,execution_predictor, transfer_predictor):
    # pure dag, id_to_task,
    d_heft_sch = DynamicHEFTScheduling(execution_predictor=execution_predictor, transfer_predictor=transfer_predictor)
    d_heft_sch.update_resource(graphHelper)
    check_sufficiency_of_info = d_heft_sch.check_sufficiency_of_info()
    if not check_sufficiency_of_info:
        raise Exception("Insufficient information for dynamic heft scheduling")
    execution_order_id = d_heft_sch.dynamic_heft()
    ratio_result = d_heft_sch.calculate_performance_ratio()
    execution_order = Queue()
    id_list = []
    execution_time_list = []
    for task_id in execution_order_id:
        task_record = d_heft_sch.id_to_task[task_id]
        execution_order.put(task_record)
    return execution_order, check_sufficiency_of_info,ratio_result
    