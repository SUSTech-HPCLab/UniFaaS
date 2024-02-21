import numpy as np
import heapq
from queue import Queue
from unifaas.dataflow.helper.graph_helper import GraphHelper


class HybridScheduling():

    def __init__(self,execution_predictor):
        self.init_resource()
        self.predictor = execution_predictor
        self.important_task_percentage = 0.05
        self.advanced = False
        
    
    def init_resource(self):
        self.id_to_task = {}
        self.fu_to_task = {}
        self.id_dag = {}
        self.source_nodes = []
        self.pure_dag = {}
        self.indegree_dict = {}
        self.func_dict = {}


    def calculate_peak_task_count(self):
        peak_task_count = len(self.source_nodes)
        tmp_indegree_dict = self.indegree_dict.copy()
        q = Queue()
        for node in self.source_nodes:
            q.put(node)
        while not q.empty():
            q_size = q.qsize()
            peak_task_count = max(peak_task_count, q_size)
            for i in range(q_size):
                node = q.get()
                for child in self.id_dag[node]:
                    tmp_indegree_dict[child] -= 1
                    if tmp_indegree_dict[child] == 0:
                        q.put(child)
        return peak_task_count

    
    def update_resource(self, graphHelper):
        self.init_resource()
        self.id_dag, self.source_nodes, self.id_to_task, self.fu_to_task,\
            self.indegree_dict  = GraphHelper.generate_dag_info(graphHelper)

    def calculate_time_factor_for_all_tasks(self):
        time_factor = {}
        for task_id in self.id_to_task.keys():
            time_factor[task_id] = 0

        if not self.advanced :
            self.time_factor = time_factor
            return 

        q = Queue()
        min_execution_time = float('inf')
        max_execution_time = float('-inf')
        for node in self.source_nodes:
            q.put(node)
        while not q.empty():
            node = q.get()
            task_record = self.id_to_task[node]
            if 'predict_output' not in task_record.keys():
                (func_name, input_size, predict_execution, predict_output) = self.predictor.predict(task_record)
            
            predict_execution = sum(task_record['predict_execution'].values())/len(task_record['predict_execution'])
            task_record['avg_execution_time'] = predict_execution
            if predict_execution < min_execution_time:
                min_execution_time = predict_execution
            if predict_execution > max_execution_time:
                max_execution_time = predict_execution

            for child in self.id_dag[node]:
                q.put(child)

        for key in time_factor.keys():
            task_record = self.id_to_task[key]
            if 'avg_execution_time' not in task_record.keys():
                (func_name, input_size, predict_execution, predict_output) = self.predictor.predict(task_record)
                predict_execution = sum(task_record['predict_execution'].values())/len(task_record['predict_execution'])
                task_record['avg_execution_time'] = predict_execution

            execution_time = task_record['avg_execution_time']
            if max_execution_time - min_execution_time == 0:
                time_factor[key] = 0
            else:
                time_factor[key] = (execution_time - min_execution_time) / (max_execution_time - min_execution_time)

        self.time_factor = time_factor
        return 
    

    def calculate_priority_for_task(self, node_id):
        pq_value = 0
        for v in self.id_dag[node_id]:
            pq_value += 1/self.indegree_dict[v]
        pq_value =  pq_value * (1-self.time_factor[node_id])
        self.id_to_task[node_id]['ic_priority'] = pq_value
        return pq_value

    def remove_node_from_graph(self, node):
        children = self.id_dag[node]
        for child in children:
            self.indegree_dict[child] -= 1
        return

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
    
    

    def ic_optimal_based_on_priority(self):
        # Invoked after update_resource
        # Output: a list of task id, execution order which can achieve IC optimal
        self.calculate_time_factor_for_all_tasks()
        pq = []
        
        execution_order_id = []
        for node in self.source_nodes:
            # -1: beacuse python heapq is a min heap
            heapq.heappush(pq, (-1*self.calculate_priority_for_task(node), node))

        while len(pq) > 0:
            front = heapq.heappop(pq)
            task_id = front[1]
            execution_order_id.append(task_id)
            self.remove_node_from_graph(task_id)
            for child in self.id_dag[task_id]:
                if self.indegree_dict[child] == 0:
                    p_val = self.calculate_priority_for_task(child)
                    heapq.heappush(pq, (-1*p_val, child))

        return execution_order_id




def generate_path_matrix(dag):
    # NOT USED/Deprecated
    graph_node_num = len(dag.keys())
    path_matrix = np.zeros((graph_node_num,graph_node_num))
    for key in dag:
        key_id = key.task_def['id']
        for val in dag[key]:
            val_id = val.task_def['id']
            path_matrix[key_id][val_id] = 1

    # Since its N^3 complexity, it should be used by small graph
    matrix_size = path_matrix.shape[0]
    for i in range(matrix_size):
        for j in range(matrix_size):
            if path_matrix[i][j] == 1:
                for k in range(matrix_size):
                    if path_matrix[j][k] == 1:
                        path_matrix[i][k] = 1

    return path_matrix


def find_meg(m : np.ndarray) -> np.ndarray:
    """
    NOT USED/Deprecated
    Find a minimal equivalent graph of M
    Ref: An algorithm for finding a minimal equivalent graph of a digraph.
        Harry T. Hsu (1975) Journal of the Assoclatlon for Computing Machinery
    """
    matrix_size = m.shape[0]
    for i in range(matrix_size):
        if m[i][i]:
            # Handle self-looping
            m[i][i] = 0
    for j in range(matrix_size):
        for i in range(matrix_size):
            if m[i][j]:
                for k in range(matrix_size):
                    m[i][k] = 0 if m[j][k] else m[i][k]
    return m

def generate_dag_by_meg(meg : np.ndarray) -> dict:
    # NOT USED/Deprecated
    id_dag = {}
    for i in range(meg.shape[0]):
        id_dag[i] = []
        for j in range(meg.shape[1]):
            if meg[i][j] == 1:
                id_dag[i].append(j)
    return id_dag



def hybrid_scheduling(graphHelper,execution_predictor):
    # pure dag, id_to_task,
    hybrid_sch = HybridScheduling(execution_predictor=execution_predictor)
    hybrid_sch.update_resource(graphHelper)
    check_sufficiency_of_info = hybrid_sch.check_sufficiency_of_info()
    hybrid_sch.advanced = check_sufficiency_of_info
    peak_count = hybrid_sch.calculate_peak_task_count()
    execution_order_id = hybrid_sch.ic_optimal_based_on_priority()
    execution_order = Queue()
    id_list = []
    execution_time_list = []
    for task_id in execution_order_id:
        task_record = hybrid_sch.id_to_task[task_id]
        execution_order.put(task_record)
        if 'avg_execution_time' in task_record.keys():
            id_list.append(task_id)
            execution_time_list.append(-task_record['avg_execution_time'])

    important_task_list = []
    if len(id_list) > 0:
        total_num = len(id_list)
        sorted_pairs = sorted(zip(id_list, execution_time_list), key=lambda x: x[1])
        for i in range(int(total_num* hybrid_sch.important_task_percentage)):
            if i < len(sorted_pairs):
                task_id = sorted_pairs[i][0]
                task_record = hybrid_sch.id_to_task[task_id]
                task_record['important'] = True
                important_task_list.append(task_record)

        
    return execution_order, check_sufficiency_of_info, peak_count, important_task_list
    