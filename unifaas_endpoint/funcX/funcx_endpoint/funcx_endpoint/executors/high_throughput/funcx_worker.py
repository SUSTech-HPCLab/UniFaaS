#!/usr/bin/env python3

import argparse
import logging
import os
import pickle
import sys
import inspect
import psutil

import zmq
from parsl.app.errors import RemoteExceptionWrapper

from funcx import set_file_logger
from funcx.serialize import FuncXSerializer
from funcx_endpoint.executors.high_throughput.messages import Message
from funcx.sdk.file import RemoteFile, RemoteDirectory
import time
from memory_profiler import memory_usage



def judge_and_set(target):
    """
    Judge a file whether it is a globus file or not.
    If it is, set the file size.
    """
    if isinstance(target, RemoteFile):
        try:
            file_size = os.path.getsize(target.get_remote_file_path())
            target.set_file_size(file_size)
        except:
            target.set_file_size(-1)
    if isinstance(target, RemoteDirectory):
        try:
            directory_size = 0
            for root, dirs, files in os.walk(target.get_remote_directory()):
                directory_size += sum([os.path.getsize(os.path.join(root, name)) for name in files])
            target.set_directory_size(directory_size)
        except:
            target.set_directory_size(-1)


def get_output_size(result):
    """
    Get the size of the result
    """
    if isinstance(result, tuple) or isinstance(result, list):
        size = 0
        for element in result:
            size += get_size_of_object(element)
        return size
    else:
        return get_size_of_object(result)


def get_function_input_size(*args, **kwargs):

    args_size = 0
    for arg in args:
        """
        The args could consist of a tuple and a dict
        e.g. args = [(RemoteFile,1,2) , {"size":1} ]
        """
        if isinstance(arg, tuple) or isinstance(arg, list):
            for e in arg:
                args_size += get_size_of_object(e)
        elif isinstance(arg, dict):
            for value in arg.values():
                args_size += get_size_of_object(value)
        elif isinstance(arg, RemoteFile):
            args_size += arg.file_size
        elif isinstance(arg, RemoteDirectory):
            args_size += arg.directory_size
        else:
            args_size += sys.getsizeof(arg)
    kwargs_size = 0
    for value in kwargs.values():
        kwargs_size += get_size_of_object(value)
    return args_size + kwargs_size

def get_size_of_object(target_obj):
    if isinstance(target_obj, tuple) or isinstance(target_obj, list):
        """
        since the size of a tuple is not the sum of all elements
        only when there is a RemoteFile, we add elements up
        otherwise, using sys.getsizeof
        """
        exists_file = False
        size = 0
        for e in target_obj:
            if isinstance(e, RemoteFile):
                size += e.file_size
                exists_file = True
            elif isinstance(e, RemoteDirectory):
                size += e.directory_size
                exists_file = True
            else:
                size += sys.getsizeof(e)
        if not exists_file:
            return sys.getsizeof(target_obj)
        else:
            return size
    elif isinstance(target_obj, RemoteFile):
        return target_obj.file_size
    elif isinstance(target_obj, RemoteDirectory):
        return target_obj.directory_size
    else :
        return sys.getsizeof(target_obj)
    


def set_output_globus_instance_size(result):
    """
    For handling RemoteFile stored in a list
    """
    if isinstance(result, tuple) or isinstance(result, list):
        for element in result:
            judge_and_set(element)
    else:
        judge_and_set(result)


def timer(func):
    def wrapper(*args, **kwargs):
        info_dict = {'input_size': get_function_input_size(args, kwargs)}
        #mem_stat = psutil.virtual_memory()
        #info_dict['mem_avaliable'] =  mem_stat.available
        info_dict['cpu_percent'] = psutil.cpu_percent()
        info_dict['mem_avaliable'] =  0
        start_time = time.time()
        # execute the function and return the maximum memory usage
        #res = memory_usage((func, args, kwargs), retval=True, max_usage=True, max_iterations=1)
        res = func(*args, **kwargs)
        end_time = time.time()
        # info_dict['result'] = res[1]
        # set_output_globus_instance_size(res[1])
        # info_dict['output_size'] = get_output_size(res[1])
        # info_dict['execution_time'] = end_time - start_time
        # info_dict['mem_usage'] = res[0]

        info_dict['result'] = res
        set_output_globus_instance_size(res)
        info_dict['output_size'] = get_output_size(res)
        info_dict['execution_time'] = end_time - start_time
        info_dict['mem_usage'] = 0


        info_dict['func_name'] = func.__name__
        info_dict['cpu_cores'] = psutil.cpu_count(logical=True)
        info_dict['cpu_freqs_max'] = psutil.cpu_freq().max
        info_dict['cpu_freqs_min'] = psutil.cpu_freq().min
        info_dict['cpu_freqs_current'] = psutil.cpu_freq().current
        return info_dict

    return wrapper


class MaxResultSizeExceeded(Exception):
    """
    Result produced by the function exceeds the maximum supported result size
    threshold of 512000B"""

    def __init__(self, result_size, result_size_limit):
        self.result_size = result_size
        self.result_size_limit = result_size_limit

    def __str__(self):
        return (
            f"Task result of {self.result_size}B exceeded current "
            f"limit of {self.result_size_limit}B"
        )


class FuncXWorker:
    """The FuncX worker
    Parameters
    ----------

    worker_id : str
     Worker id string

    address : str
     Address at which the manager might be reached. This is usually 127.0.0.1

    port : int
     Port at which the manager can be reached

    logdir : str
     Logging directory

    debug : Bool
     Enables debug logging

    result_size_limit : int
     Maximum result size allowed in Bytes
     Default = 10 MB == 10 * (2**20) Bytes


    Funcx worker will use the REP sockets to:
         task = recv ()
         result = execute(task)
         send(result)
    """

    def __init__(
            self,
            worker_id,
            address,
            port,
            logdir,
            debug=False,
            worker_type="RAW",
            result_size_limit=512000,
    ):

        self.worker_id = worker_id
        self.address = address
        self.port = port
        self.logdir = logdir
        self.debug = debug
        self.worker_type = worker_type
        self.serializer = FuncXSerializer()
        self.serialize = self.serializer.serialize
        self.deserialize = self.serializer.deserialize
        self.result_size_limit = result_size_limit

        global logger
        logger = set_file_logger(
            os.path.join(logdir, f"funcx_worker_{worker_id}.log"),
            name="worker_log",
            level=logging.DEBUG if debug else logging.INFO,
        )

        logger.info(f"Initializing worker {worker_id}")
        logger.info(f"Worker is of type: {worker_type}")

        if debug:
            logger.debug("Debug logging enabled")

        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.identity = worker_id.encode()

        self.task_socket = self.context.socket(zmq.DEALER)
        self.task_socket.setsockopt(zmq.IDENTITY, self.identity)

        logger.info(f"Trying to connect to : tcp://{self.address}:{self.port}")
        self.task_socket.connect(f"tcp://{self.address}:{self.port}")
        self.poller.register(self.task_socket, zmq.POLLIN)

    def registration_message(self):
        return {"worker_id": self.worker_id, "worker_type": self.worker_type}

    def start(self):

        logger.info("Starting worker")

        result = self.registration_message()
        task_type = b"REGISTER"
        logger.debug("Sending registration")
        self.task_socket.send_multipart(
            [task_type, pickle.dumps(result)]  # Byte encoded
        )

        while True:

            logger.debug("Waiting for task")
            p_task_id, p_container_id, msg = self.task_socket.recv_multipart()
            task_id = pickle.loads(p_task_id)
            container_id = pickle.loads(p_container_id)
            logger.debug(f"Received task_id:{task_id} with task:{msg}")

            result = None
            task_type = None
            if task_id == "KILL":
                task = Message.unpack(msg)
                if task.task_buffer.decode("utf-8") == "KILL":
                    logger.info("[KILL] -- Worker KILL message received! ")
                    task_type = b"WRKR_DIE"
                else:
                    logger.exception(
                        "Caught an exception of non-KILL message for KILL task"
                    )
                    continue
            else:
                logger.debug("Executing task...")

                try:
                    result = self.execute_task(msg)
                    serialized_result = self.serialize(result)

                    if sys.getsizeof(serialized_result) > self.result_size_limit:
                        raise MaxResultSizeExceeded(
                            sys.getsizeof(serialized_result), self.result_size_limit
                        )
                except Exception as e:
                    logger.exception(f"Caught an exception {e}")
                    result_package = {
                        "task_id": task_id,
                        "container_id": container_id,
                        "exception": self.serialize(
                            RemoteExceptionWrapper(*sys.exc_info())
                        ),
                    }
                else:
                    logger.debug("Execution completed without exception")
                    result_package = {
                        "task_id": task_id,
                        "container_id": container_id,
                        "result": serialized_result,
                    }
                result = result_package
                task_type = b"TASK_RET"

            logger.debug("Sending result")

            self.task_socket.send_multipart(
                [task_type, pickle.dumps(result)]  # Byte encoded
            )

            if task_type == b"WRKR_DIE":
                logger.info(f"*** WORKER {self.worker_id} ABOUT TO DIE ***")
                # Kill the worker after accepting death in message to manager.
                sys.exit()
                # We need to return here to allow for sys.exit mocking in tests
                return

        logger.warning("Broke out of the loop... dying")

    def execute_task(self, message):
        """Deserialize the buffer and execute the task.

        Returns the result or throws exception.
        """
        task = Message.unpack(message)
        f, args, kwargs = self.serializer.unpack_and_deserialize(
            task.task_buffer.decode("utf-8")
        )
        return timer(f)(*args, **kwargs)


def cli_run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-w", "--worker_id", required=True, help="ID of worker from process_worker_pool"
    )
    parser.add_argument(
        "-t", "--type", required=False, help="Container type of worker", default="RAW"
    )
    parser.add_argument(
        "-a", "--address", required=True, help="Address for the manager, eg X,Y,"
    )
    parser.add_argument(
        "-p",
        "--port",
        required=True,
        help="Internal port at which the worker connects to the manager",
    )
    parser.add_argument(
        "--logdir", required=True, help="Directory path where worker log files written"
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Directory path where worker log files written",
    )

    args = parser.parse_args()
    worker = FuncXWorker(
        args.worker_id,
        args.address,
        int(args.port),
        args.logdir,
        worker_type=args.type,
        debug=args.debug,
    )
    worker.start()
    return


if __name__ == "__main__":
    cli_run()
