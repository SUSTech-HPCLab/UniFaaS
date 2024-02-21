from queue import Queue
from enum import Enum
import time
from unifaas.dataflow.states import States


class GraphHelper:

    def __init__(self, idle_threshold=10):
        self.raw_graph = {}  # {task1:[successor1, successor2]}
        self.workflow_graph = {}  # {task1_id:[successor1_id, successor2_id]}
        self.future_to_task = {}
        self.id_to_task = {}
        self.idle_threshold = idle_threshold
        self.task_idle_since = None
        self.entry_task_queue = Queue()
        self.task_queue = Queue()

    
    @staticmethod
    def generate_dag_info(graphHelper):
    # Generate a pure dag
        while time.time() - graphHelper.get_task_idle_since() < 5:
            time.sleep(1)
        cur_qsize = graphHelper.task_queue.qsize()
        tot_qsize = cur_qsize
        pure_dag = {}
        id_dag = {}
        fu_to_task = {}
        id_to_task = {}
        indegree_dict = {}
        source_nodes = []
        while cur_qsize > 0:
            task_record = graphHelper.task_queue.get()
            app_fu = task_record['app_fu']
            task_id = task_record['id']
            id_to_task[task_id] = task_record
            fu_to_task[app_fu] = task_record
            indegree_dict[task_id] = len(task_record['depends'])
            if len(task_record['depends']) == 0:
                source_nodes.append(task_id)
            if app_fu not in pure_dag.keys():
                pure_dag[app_fu] = []
                id_dag[task_id] = []

            for dep in task_record['depends']:
                # make sure pred node is not scheduled.
                dep_id = dep.task_def['id']
                if dep.task_def['status'] == States.scheduling: 
                    if dep in pure_dag.keys():
                        pure_dag[dep].append(app_fu)
                        id_dag[dep_id].append(task_id)
                    else:
                        pure_dag[dep] = [app_fu]
                        id_dag[dep_id] = [task_id]
            cur_qsize -= 1

        return id_dag, source_nodes, id_to_task, fu_to_task, indegree_dict


    def _insert_to_raw_graph(self, task):
        if self.raw_graph.get(task['app_fu']) is None:
            self.workflow_graph[task['id']] = []
            self.raw_graph[task['app_fu']] = []
        for dep in task['depends']:
            dep_task = self.future_to_task[dep]
            self.workflow_graph[dep_task['id']].append(task['id'])
            self.raw_graph[dep].append(task['app_fu'])

    def put_scheduling_task(self, task):
        if len(task['depends']) <= 0:
            self.entry_task_queue.put(task)
        app_fu = task['app_fu']
        self.future_to_task[app_fu] = task
        self.id_to_task[task['id']] = task
        self.task_queue.put(task)
        self.task_idle_since = time.time()
        self._insert_to_raw_graph(task)

    def is_task_queue_empty(self):
        return self.task_queue.empty()
    
    def update_task_idle_since(self):
        self.task_idle_since = time.time()
    
    def get_task_idle_since(self):
        if self.task_idle_since is None:
            return float("inf")
        return self.task_idle_since

    def analyze_level(self, workflow_graph=None):
        if not workflow_graph:
            workflow_graph = self.workflow_graph
        indegree = { u : 0 for u in workflow_graph.keys()}
        for u in workflow_graph.keys():
            for v in workflow_graph[u]:
                indegree[v] += 1
        counter = 0
        levels = []
        tot_nodes = len(indegree)
        while counter < tot_nodes:
            next_level = []
            for u in indegree.keys():
                if indegree[u] == 0:
                    next_level.append(u)
            for u in next_level:
                for v in workflow_graph[u]:
                    indegree[v] -= 1
                del indegree[u]
            levels.append(next_level)
            counter += len(next_level)
        return levels
    

    def get_busy_status(self):
        # If there is no more incoming tasks in the last self.idle_threshold seconds
        # the workflow is considered idle (return False)
        if self.task_idle_since is None:
            return True

        if time.time() - self.task_idle_since > self.idle_threshold:
            return False
        else:
            return True

    def generate_graph_of_unfinished_tasks(self):
        # the output graph should be used for HEFT scheduling
        # format {appfu: [successor1, successor2]}
        unfinished_graph = {}
        unfinished_fu_to_task = {}
        for app_fu in self.raw_graph.keys():
            task_record = self.future_to_task[app_fu]
            if task_record['status'] <= States.data_managing:
                unfinished_graph[app_fu] = []
                unfinished_fu_to_task[app_fu] = task_record
                for successor in self.raw_graph[app_fu]:
                    successor_task_record = self.future_to_task[successor]
                    if successor_task_record['status'] < States.data_managing:
                        unfinished_graph[app_fu].append(successor)
        return unfinished_graph, unfinished_fu_to_task


    def get_workflow_graph(self):
        return self.workflow_graph, self.id_to_task

class NodeType(Enum):
    ENTRY = 1
    INTERMEDIATE = 2
    CRITICAL = 3
    EXIT = 4


class DAGNode:
    def __init__(self, app_fu, node_type=NodeType.INTERMEDIATE):
        self.app_fu = app_fu
        self.computation_time = 1
        self.node_type = node_type

    def __eq__(self, other):
        return self.node_type == other.node_type \
               and self.app_fu == other.app_fu \
               and self.computation_time == other.computation_time

    def __hash__(self):
        return hash(self.app_fu)

    def set_node_type(self, node_type):
        self.node_type = node_type


class DAGEdge:
    def __init__(self, start_node, dest_node, weight):
        self.dest_node = dest_node
        self.weight = weight
        self.start_node = start_node


graphHelper = GraphHelper()
