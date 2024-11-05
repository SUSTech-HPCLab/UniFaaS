from unifaas.dataflow.states import States
import time
import logging
from concurrent.futures import Future
from queue import PriorityQueue
from queue import Queue
from unifaas.dataflow.helper.graph_helper import graphHelper
import threading
from unifaas.compressor import compress_func, SUPPORT_COMPRESSOR, decompress_func

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
        compress_predictor,
        data_manager,
        priority_type,
    ):
        self.resource_poller = resource_poller
        self.execution_predictor = execution_predictor
        self.transfer_predictor = transfer_predictor
        self.compress_predictor = compress_predictor
        self.data_manager = data_manager
        self.endpoint_performance_ratio = None
        self.priority_type = priority_type
        self.scheduling_queue = Queue()  # scheduling queue
        self.data_ready_queue = PriorityQueue()  # ic_optimal priority queue
        self.special_decompress_launch_que = Queue()


        self._kill_event = threading.Event()

        self._launch_decompress_task_thread = threading.Thread(
                target=self._launch_decompress_task,
                args=(self._kill_event,),
                name="Launch-Decompress-Task-Thread",
            )
        self._launch_decompress_task_thread.daemon = True
        self._launch_decompress_task_thread.start()




        self.tmp_dfk   = None
     

    def update_performance_ratio(self, ratio_result):
        if self.endpoint_performance_ratio is None:
            self.endpoint_performance_ratio = ratio_result

    def put_task_record(self, task_record):
        task_with_priority = TaskWithPriority(task_record)
        self.data_ready_queue.put(task_with_priority)

    def fetch_task_record(self):

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

            task_record = self.fetch_task_record()
            if task_record is None:
                break

            # 防止重复提交任务
            if task_record.get('dheft_compress_target_checked', False):
                continue


            if "heft_priority" in task_record.keys():
                exp_logger.debug(
                    f"[DHEFT] task {task_record['id']} {task_record['func_name']} with priority {task_record['heft_priority']} "
                )

            if task_record["status"] == States.scheduling:
    
                # 开发中 157-161
                target_ep = self.select_endpoint(task_record, feasible_ep) #don't select endpoint for a compress task
                if not task_record["never_change"]:
                        task_record["executor"] = target_ep
                
                # TODO：替换成所有需要压缩的任务
                # TODO: 需要确保只压缩一次
                if task_record['func_name'] == 'file_task2':
                    compress_flag = self.compress_data_if_necessary(task_record)
                    if compress_flag:
                        # 如果决定压缩之后，需要走特殊的提交通道
                        continue


                self.resource_poller.update_status_when_submit_one_task(
                    task_record['executor'], task_record=task_record
                )
                task_record["submitted_to_poller"] = True
                task_record["status"] = States.data_managing
                self.data_manager.group_transfer(task_record)
        time.sleep(
            7
        )  # sleep for 7 seconds to avoid too short interval between two scheduling


    def check_if_compress(self, parent_task, task_record):
        # 需要选择一个最好的压缩函数
        # 压缩收益等于 : diff_size / avg_band - predict_compress_time - avg_predict_decompress_time
        # 首先计算压缩收益
        parent_output = parent_task['output_size']
        parent_executor = parent_task['executor']

        max_saved_time = 0
        best_method = None

        for compress_method in SUPPORT_COMPRESSOR:
            func_name = parent_task['func_name']
            cur_executor  = task_record['executor']
            compressed_size = self.compress_predictor.predict_output_size(func_name,compress_method, parent_output)
            if compressed_size is None:
                continue
            diff_size = max(0, parent_output - compressed_size)
            saved_time = max(0, self.transfer_predictor.perdict_based_on_bandwith(parent_executor, cur_executor, diff_size))
          
            compress_time = self.compress_predictor.predict_compress_execution_time(func_name, compress_method, parent_output, parent_executor, 'compress')
            decompress_time = self.compress_predictor.predict_compress_execution_time(func_name, compress_method, compressed_size, cur_executor, 'decompress')
            if saved_time is None or  compress_time is None or decompress_time is None:
                continue
            saved_time -= compress_time + decompress_time
            if saved_time  > max_saved_time:
                max_saved_time = saved_time
                best_method = compress_method
                # TODO : 如果saved 不够多需要重新检查
            
        return best_method


   



    def compress_data_if_necessary(self, task_record):
        #这个函数用于判断是否需要压缩某一次传输, task_record是一个target task
        if self.tmp_dfk is None:
            from unifaas.dataflow.dflow import DataFlowKernelLoader
            self.tmp_dfk = DataFlowKernelLoader.dfk()


        #选择需要压缩的depend，并将对应的dep标记为dheft_compress。此处所有的dep 必须是
        tmp_decompress_task_tbl = {}

        for dep in task_record['depends']:
            parent_task = dep.task_def
           
            
            # TODO: 变成排他的endpoint
            # if  parent_task['executor'] == task_record['executor']:
            #     # 同一个executor不进行任何操作
            #     continue

            # 如果发现parent已经被check过了，就不能再check了，直接跟随之前的选择，如果需要压缩，则压缩，若不需要压缩则不压缩
            if not parent_task.get('dheft_compress_source_checked', False):
                # 添加compress_flag告诉所有子任务，需要传输压缩后的数据/或者已经开始传输了，不能再压缩了
                # TODO: 变成智能选择,此处根据parent的输出和当前task_record的条件，确定是否进行压缩。以及所选择的压缩方式
                compress_method = self.check_if_compress(parent_task, task_record)

                compress_method = 'gzip'

                # TODO: 为了debug暂时不开启智能xuanze
                if compress_method is not None:
                    parent_task['compress_option'] = (compress_method, None, None, None)
                    compress_app = self.tmp_dfk.internal_submit(func=compress_func, app_args=tuple([parent_task['app_fu'],compress_method]), compress_option=(None, compress_method, None,None))
                    parent_task['dheft_source_to_compress_app'] = (compress_method, compress_app)
                    self.schedule_compress_task(compress_app)
                    self._direct_launch_task(compress_app.task_def)


            # 追随之前的选择，如果之前进行压缩了，则需要压缩 (除非是在同一个endpoint上)
            if 'dheft_source_to_compress_app' in parent_task:
                compress_method  = parent_task['dheft_source_to_compress_app'][0]
                compress_app  = parent_task['dheft_source_to_compress_app'][1]
                de_compress_app = self.tmp_dfk.internal_submit(func=decompress_func, app_args=tuple([compress_app,compress_method]), compress_option=(None, None, compress_method,None))
                de_compress_app.task_def['dheft_compress_app'] = compress_app
                de_compress_app.task_def['executor'] =  task_record['executor']

                tmp_decompress_task_tbl[parent_task['app_fu']] = de_compress_app
                for i in range(len(task_record['depends'])):
                        if task_record['depends'][i] == parent_task['app_fu']:
                            task_record['depends'][i] = de_compress_app

            parent_task['dheft_compress_source_checked'] = True

        task_record['dheft_compress_target_checked'] = True

        # dheft_compress_source_checked 是指：source任务的输出是否需要被压缩
        # dheft_compress_target_checked 是指：target任务已经被检验过，是否需要被压缩了

        if len(tmp_decompress_task_tbl) == 0:
            return False

        
        # 替换所有的args/kwargs
        compress_args = []
        for tmp_arg in task_record['args']:
            if tmp_arg in tmp_decompress_task_tbl:
                compress_args.append(tmp_decompress_task_tbl[tmp_arg])
            else:
                compress_args.append(tmp_arg)
        task_record['args'] = tuple(compress_args)

        compress_kwargs = {}
        for tmp_key in task_record['kwargs']:
            dep = task_record['kwargs']
            if dep in tmp_decompress_task_tbl:
                compress_kwargs[tmp_key] = tmp_decompress_task_tbl[dep]
            else:
                compress_kwargs[tmp_key] = dep
        task_record['kwargs'] = compress_kwargs
        task_record['compress_option'] = (None,None,None,True)

        # 把这个target record 加入一个队列中，需要处理compress_app的逻辑
        self.special_decompress_launch_que.put(task_record)

        return True



        

    def _direct_launch_task(self, task_record):
        # 这个函数用于直接启动compress任务
        task_record["submitted_to_poller"] = True
        task_record["status"] = States.data_managing
        self.data_manager.group_transfer(task_record)
        return
    

    

    def _launch_decompress_task(self,kill_event):
        while not kill_event.is_set():
            qsize = self.special_decompress_launch_que.qsize()
            while qsize > 0:
                task_record = self.special_decompress_launch_que.get()

                # 首先检查 decompress 任务是否launch
                for dep in task_record['depends']:
                    if dep.task_def['compress_option'][2] is not None and not dep.task_def.get('dheft_decompress_launch', False):
                        # 查看decompress 任务的依赖（压缩任务是否ready） 如果ready的话启动解压任务（传输数据）
                        if dep.task_def['dheft_compress_app'].done():
                            # 解压任务的executor 在创建时被指定                            
                            self._direct_launch_task(dep.task_def)
                            dep.task_def['dheft_decompress_launch'] = True
                # 再检查 dep是不是都ready了
                all_done = True
                for dep in task_record['depends']:
                    if not dep.done():
                        all_done = False
                        break
                # dep ready之后直接launch 否则重新放入监控队列
                if all_done:
                    self.resource_poller.update_status_when_submit_one_task(
                    task_record['executor'], task_record=task_record
                    )
                    self._direct_launch_task(task_record)
                else:
                    self.special_decompress_launch_que.put(task_record)
                qsize -= 1

            time.sleep(0.5)       


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
        

