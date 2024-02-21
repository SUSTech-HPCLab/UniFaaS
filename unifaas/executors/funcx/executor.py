import threading
import queue
import time
import logging
import random
import os
import inspect
from concurrent.futures import Future
from unifaas.utils import RepresentationMixin
from unifaas.executors.status_handling import NoStatusHandlingExecutor
from unifaas.executors.errors import (
    BadMessage, ScalingFailed,
    DeserializationError, SerializationError,
    UnsupportedFeatureError
)
from unifaas.app.errors import RemoteExceptionWrapper
from concurrent.futures import ThreadPoolExecutor
from funcx.sdk.client import FuncXClient
from funcx.sdk.file import RemoteFile, RemoteDirectory
from functools import partial

logger = logging.getLogger("unifaas")
exp_logger = logging.getLogger("experiment")


def _scale_out_dummy_task(managers=1,workers=1):
    return "scale out"

def _dummy_task_for_transfer_profile():
    # generate uuid
    file_list = []
    import uuid
    # create 5m, 40m 100mb file by subprocess
    size_list = [5, 40, 100]

    for size in size_list:
        func_uuid = str(uuid.uuid4())
        import subprocess
        from funcx.sdk.file import RsyncFile
        remote_file = RsyncFile.remote_generate(func_uuid)
        subprocess.run(["dd", "if=/dev/urandom", "of={}".format(remote_file.file_path), f"bs=1024", f"count={size*1024}"])
        file_list.append(remote_file)
    return file_list
  

class FuncXExecutor(NoStatusHandlingExecutor, RepresentationMixin):

    def __init__(self,
                 label="FuncXExecutor",
                 # provider=None,
                 endpoint=None,
                 workdir='FXEX',
                 batch_interval=3,
                 batch_size=1000,
                 poll_interval=5,
                 submit_workers=2,
                 poll_workers=1,
                 managed=True,
                 funcx_service_address="https://api2.funcx.org/v2",
                 pre_data_trans=False,
                 dev_mode=False,):

        logger.info("Initializing FuncXExecutor")
        self.label = label
        self.endpoint = endpoint
        self.submit_workers = submit_workers
        self.submit_pool = ThreadPoolExecutor(max_workers=self.submit_workers)
        self.poll_workers = poll_workers
        self.batch_interval = batch_interval
        self.dev_mode = dev_mode
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.workdir = os.path.abspath(workdir)
        self.managed = managed
        self.pre_data_trans = pre_data_trans
        self.funcx_service_address = funcx_service_address
        self.remote_scale_out_times = 0
        self._scaling_enabled = False
        self._counter_lock = threading.Lock()
        self._task_counter = 0
        self._tasks = {}
        self.batch_exp_start_time = None

    def start(self):
        """ Called when DFK starts the executor when the config is loaded
        1. Make workdir
        2. Start task_status_poller
        3. Create a funcx SDK client
        """
        os.makedirs(self.workdir, exist_ok=True)
        self.task_outgoing = queue.Queue()
        # Create a funcx SDK client
        if self.dev_mode:
            # The initalization of funcx client is not needed in dev mode
            # Because it require a running funcx web service
            self.fxc = None
        else:
            self.fxc = FuncXClient(funcx_service_address=self.funcx_service_address, need_transfer=True)


        # Create a dict to record the function uuids for apps
        self.functions = {}
        # Record the mapping between funcX function uuids and internal task ids
        self.task_uuids = {}

        return []

    def set_blocks_info(self, max_blocks, min_blocks):
        self.max_blocks = max_blocks
        self.min_blocks = min_blocks
    

    def start_with_status_poller(self, status_poller):
        """start with the status poller.
            This function must be called after status_poller is initialized.
            It can't be called in the __init__ function because the initialization of status_poller needs the executor info.
            Therefore, add_executors -> init ResoucreStatusPoller -> init_status_poller at executor
        """
        self.status_poller = status_poller
        self._kill_event = threading.Event()
        # Start the task submission thread
        self.task_submit_thread_instance = threading.Thread(target=self.task_submit_thread,
                                                   args=(self._kill_event,))
        self.task_submit_thread_instance.daemon = True
        self.task_submit_thread_instance.start()
        logger.info("Started task submit thread")

        # Start the task status poller thread
        self.task_poller_thread = threading.Thread(target=self.task_status_poller,
                                                   args=(self._kill_event,))
        self.task_poller_thread.daemon = True
        self.task_poller_thread.start()
        logger.info("Started task status poller thread")

    def submit(self, func, resource_specification, *args, **kwargs):
        """Submits work to the the task_outgoing queue.
        The task_outgoing queue is an external process listens on this
        queue for new work. This method behaves like a
        submit call as described here `Python docs: <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_
        Args:
            - func (callable) : Callable function
            - *args (list) : List of arbitrary positional arguments.
        Kwargs:
            - **kwargs (dict) : A dictionary of arbitrary keyword args for func.
        Returns:
            Future
        """
        if resource_specification:
            logger.error("Ignoring the resource specification. "
                         "Parsl resource specification is not supported in FuncXExecutor. "
                         "Please check WorkQueueExecutor if resource specification is needed.")
            raise UnsupportedFeatureError('resource specification', 'FuncXExecutor', 'WorkQueue Executor')
        with self._counter_lock:
            self._task_counter += 1
            task_id = self._task_counter


        # The code
        if hasattr(func, "__wrapped__"):
            orgin_func = func.__wrapped__
        else:
            orgin_func = func

        if func not in self.functions:
            try:
                logger.info("Registering Parsl app {}".format(func.__name__))
                func_uuid = self.fxc.register_function(orgin_func,
                                                       description="Parsl app {}".format(func.__name__))
                logger.info("Registered Parsl app {} as funcX function uuid {}".format(func.__name__,
                                                                                       func_uuid))
            except Exception:
                logger.error("Error in registering Parsl app {}".format(func.__name__))
                raise Exception("Error in registering Parsl app {}".format(func.__name__))
            else:
                self.functions[func] = func_uuid

        func_uuid = self.functions[func]

        # handle people sending blobs gracefully
        args_to_print = args
        if logger.getEffectiveLevel() >= logging.DEBUG:
            args_to_print = tuple([arg if len(repr(arg)) < 100 else (repr(arg)[:100] + '...') for arg in args])
        logger.debug("Pushing function {} to queue with args {}".format(func, args_to_print))

        self.tasks[task_id] = Future()

        msg = {"task_id": task_id,
               "func_uuid": func_uuid,
               "args": args,
               "kwargs": kwargs}

        # Post task to the the outgoing queue
        self.task_outgoing.put(msg)

        # Return the future
        return self.tasks[task_id]

    def task_submit_thread(self, kill_event):
        """Task submission thread that fetch tasks from task_outgoing queue,
        batch function requests, and submit functions to funcX"""
        while not kill_event.is_set():
            messages = self._get_tasks_in_batch()
            # if messages:
            #     logger.info(f"[TASK_SUBMIT_THREAD] Submitting {len(messages)} tasks to {self.label}")
            self._schedule_tasks(messages)
        logger.info("[TASK_SUBMIT_THREAD] Exiting")

    def _schedule_tasks(self, messages):
        """Schedule a batch of tasks to different funcx endpoints
        This is a naive task scheduler, which submit a task to a random endpoint
        """
        if messages:
            batch = self.fxc.create_batch()
            num_of_msg = 0
            for msg in messages:
                # random endpoint selection for v1
                try:
                    endpoint = self.endpoint
                    func_uuid, args, kwargs = msg['func_uuid'], msg['args'], msg['kwargs']
                    remote_data_list = []
                    replace_fu_args = []
                    for arg in args:
                        if isinstance(arg, RemoteFile) or isinstance(arg, RemoteDirectory): 
                            remote_data_list.append(arg)
                        if isinstance(arg, list):
                            if len(arg) > 0 and isinstance(arg[0], Future):
                                tmp_arg = [fu.result() for fu in arg]
                                replace_fu_args.append(tmp_arg)
                                continue
                        replace_fu_args.append(arg)
                    for val in kwargs.values():
                        if isinstance(val, RemoteFile) or isinstance(val, RemoteDirectory):
                            remote_data_list.append(val)
                    
                    replace_fu_args_tuple = tuple(replace_fu_args)
                    if self.pre_data_trans:
                        remote_data = None
                    else:
                        remote_data = remote_data_list
                    batch.add(*replace_fu_args_tuple, **kwargs,
                            endpoint_id=endpoint,
                            function_id=func_uuid, remote_data=remote_data)
                    num_of_msg += 1
                    logger.debug("[TASK_SUBMIT_THREAD] Adding msg {} to funcX batch".format(msg))
                except Exception as e:
                    continue
                # Submit the batch with multiple threads
            logger.info("[TASK_SUBMIT_THREAD] Adding msg {} to funcX batch".format(num_of_msg))
            batch_future = self.submit_pool.submit(self.fxc.batch_run, batch)
            batch_future.add_done_callback(partial(self._batch_run_done, messages=messages))

    def _batch_run_done(self, batch_future, messages):
        """Callback function that is called when a batch of tasks is done"""
        try:
            batch_tasks = batch_future.result()

        except Exception as e:
            logger.error("[TASK_SUBMIT_THREAD] Error in batch_run callback: {}".format(e))
            raise Exception("Error in batch_run callback: {}".format(e))
        else:
            for i, msg in enumerate(messages):
                self.task_uuids[batch_tasks[i]] = msg['task_id']

    def _get_tasks_in_batch(self):
        """Get tasks from task_outgoing queue in batch, either by interval or by batch size"""
        messages = []
        start = time.time()
        while True:
            if time.time() - start >= self.batch_interval or len(messages) >= self.batch_size:
                break
            try:
                x = self.task_outgoing.get(timeout=0.1)
            except queue.Empty:
                break
            else:
                messages.append(x)
        if len(messages) > 0:
            if self.batch_exp_start_time is None:
                self.batch_exp_start_time = time.time()
            
        return messages

    def _batch_res_done(self,  batch_res_future):
        try:
            batch_results = batch_res_future.result()
        except Exception:
            logger.exception("[TASK_POLLER_THREAD] Exception in polling tasks in batch")
        else:
            for tid in batch_results:
                msg = batch_results[tid]
                if not msg['pending']:
                    internal_task_id = self.task_uuids.pop(tid)
                    task_fut = self.tasks.pop(internal_task_id)
                    logger.debug("[TASK_POLLER_THREAD] Processing message " 
                                "{} for task {}".format(msg, internal_task_id))

                    if 'result' in msg:
                        result_with_probe = msg['result']
                        task_fut.set_result(result_with_probe)
                    elif 'exception' in msg:
                        try:
                            s = msg['exception']
                            # s should be a RemoteExceptionWrapper... so we can reraise it
                            if isinstance(s, RemoteExceptionWrapper):
                                try:
                                    s.reraise()
                                except Exception as e:
                                    task_fut.set_exception(e)
                            elif isinstance(s, Exception):
                                task_fut.set_exception(s)
                            else:
                                raise ValueError("Unknown exception-like type received: {}".format(type(s)))
                        except Exception as e:
                            # TODO could be a proper wrapped exception?
                            task_fut.set_exception(
                                DeserializationError("Received exception, but handling also threw an exception: {}".format(e)))
                    else:
                        raise BadMessage("Message received is neither result or exception")
         

    def task_status_poller(self, kill_event):
        """Task status poller thread that keeps polling the status of existing tasks"""
        while not kill_event.is_set():
            if self.tasks and self.task_uuids:
                batch_size = self.batch_size
                sum_to_poll_tasks = list(self.task_uuids.keys())
                import math
                n_batches = int(math.ceil(len(sum_to_poll_tasks) / batch_size))

                def split_list(a, n):
                    k, m = divmod(len(a), n)
                    return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))
                
                split_tasks = list(split_list(sum_to_poll_tasks, n_batches))
                with ThreadPoolExecutor(max_workers=self.poll_workers) as poll_executor:
                    for to_poll_tasks in split_tasks:
                        # TODO: using prod funcx now, need to update to get_batch_result for results
                            batch_res_future = poll_executor.submit(self.fxc.get_batch_result, to_poll_tasks)
                            batch_res_future.add_done_callback(partial(self._batch_res_done))
            time.sleep(self.poll_interval)

        logger.info("[TASK_POLLER_THREAD] Exiting")

    def shutdown(self):
        self._kill_event.set()
        logger.info("FuncXExecutor shutdown")
        return True

    def scale_in(self):
        pass

    def handle_probe_file(self):
        func = _dummy_task_for_transfer_profile

        with self._counter_lock:
            self._task_counter += 1
            task_id = self._task_counter

        if func not in self.functions:
            try:
                func_uuid = self.fxc.register_function(func,
                                                       description="unifaas app {}".format(func.__name__))
            except Exception:
                logger.error("Error in registering unifaas app {}".format(func.__name__))
                raise Exception("Error in registering unifaas app {}".format(func.__name__))
            else:
                self.functions[func] = func_uuid
        
        func_uuid = self.functions[func]
        self.tasks[task_id] = Future()
        batch = self.fxc.create_batch()
        batch.add(endpoint_id=self.endpoint, function_id=func_uuid,)
        batch_tasks = self.fxc.batch_run(batch)
        self.task_uuids[batch_tasks[0]] = task_id
        return self.tasks[task_id]

    
    def scale_out_cmd(self, num):
        exp_logger.info("Scale out command received with num: {}".format(num))
        self.scale_out(self.status_poller, out_num=num, in_num=0)

    def scale_in_cmd(self, num):
        exp_logger.info("Scale in command received with num: {}".format(num))
        self.scale_out(self.status_poller, in_num=num, out_num=0)

    def scale_out(self, status_poller, out_num=1, in_num=0):
        # status_poller for updating real_time_status   
        func = _scale_out_dummy_task
        with self._counter_lock:
            self._task_counter += 1
            task_id = self._task_counter 
            
        if func not in self.functions:
            try:
                func_uuid = self.fxc.register_function(func,
                                                       description="unifaas app {}".format(func.__name__))
            except Exception:
                logger.error("Error in registering unifaas app {}".format(func.__name__))
                raise Exception("Error in registering unifaas app {}".format(func.__name__))
            else:
                self.functions[func] = func_uuid

        func_uuid = self.functions[func]
        self.tasks[task_id] = Future()
        self.tasks[task_id].add_done_callback(partial(self._dummy_task_call_back, status_poller=status_poller))
        self.remote_scale_out_times += 1
        batch = self.fxc.create_batch()
        batch.add(endpoint_id=self.endpoint, function_id=func_uuid, dummy=True, cmd_config = {'out_manager':out_num , 'in_manager':in_num})
        batch_tasks = self.fxc.batch_run(batch)
        self.task_uuids[batch_tasks[0]] = task_id   
        return 

    def online_scale_in(self, in_num):
        func = _scale_out_dummy_task
        with self._counter_lock:
            self._task_counter += 1
            task_id = self._task_counter 
        if func not in self.functions:
            try:
                func_uuid = self.fxc.register_function(func,
                                                       description="unifaas app {}".format(func.__name__))
            except Exception:
                logger.error("Error in registering unifaas app {}".format(func.__name__))
                raise Exception("Error in registering unifaas app {}".format(func.__name__))
            else:
                self.functions[func] = func_uuid
        func_uuid = self.functions[func]
        self.tasks[task_id] = Future()
        self.tasks[task_id].add_done_callback(partial(self._dummy_scale_in_call_back,))
        self.remote_scale_out_times += 1
        batch = self.fxc.create_batch()
        batch.add(endpoint_id=self.endpoint, function_id=func_uuid, dummy=True, cmd_config = {'out_manager':0 , 'in_manager':in_num})
        batch_tasks = self.fxc.batch_run(batch)
        self.task_uuids[batch_tasks[0]] = task_id
        return 

    def _dummy_task_call_back(self, fu, status_poller):
        result = fu.result()
        status_poller.update_real_time_status_when_open(self.label, result)
        return

    def _dummy_scale_in_call_back(self,fu):
        result = fu.result()
        return 

    @property
    def scaling_enabled(self):
        return self._scaling_enabled