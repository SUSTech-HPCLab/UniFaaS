#!/usr/bin/env python3

import argparse
import json
import logging
import math
import multiprocessing
import os
import pickle
import platform
import queue
import subprocess
import sys
import threading
import time
import uuid

import psutil
import zmq
from parsl.version import VERSION as PARSL_VERSION

from funcx import set_file_logger
from funcx.serialize import FuncXSerializer
from funcx_endpoint.executors.high_throughput.container_sched import naive_scheduler
from funcx_endpoint.executors.high_throughput.mac_safe_queue import mpQueue
from funcx_endpoint.executors.high_throughput.messages import (
    ManagerStatusReport,
    Message,
    Task,
    TaskStatusCode,
)
from funcx_endpoint.executors.high_throughput.worker_map import WorkerMap

RESULT_TAG = 10
TASK_REQUEST_TAG = 11
HEARTBEAT_CODE = (2 ** 32) - 1


logger = None


class Manager:
    """Manager manages task execution by the workers

                |         0mq              |    Manager         |   Worker Processes
                |                          |                    |
                | <-----Request N task-----+--Count task reqs   |      Request task<--+
    Interchange | -------------------------+->Receive task batch|          |          |
                |                          |  Distribute tasks--+----> Get(block) &   |
                |                          |                    |      Execute task   |
                |                          |                    |          |          |
                | <------------------------+--Return results----+----  Post result    |
                |                          |                    |          |          |
                |                          |                    |          +----------+
                |                          |                IPC-Qeueues

    """

    def __init__(
        self,
        task_q_url="tcp://127.0.0.1:50097",
        result_q_url="tcp://127.0.0.1:50098",
        max_queue_size=10,
        cores_per_worker=1,
        max_workers=float("inf"),
        uid=None,
        heartbeat_threshold=120,
        heartbeat_period=30,
        logdir=None,
        debug=False,
        block_id=None,
        internal_worker_port_range=(50000, 60000),
        worker_mode="singularity_reuse",
        container_cmd_options="",
        scheduler_mode="hard",
        worker_type=None,
        worker_max_idletime=60,
        # TODO : This should be 10ms
        poll_period=100,
        local_data_path="",
        globus_ep_id="",
        rsync_ip="",
        rsync_username="",
        check_rsync_auth="",
    ):
        """
        Parameters
        ----------
        worker_url : str
             Worker url on which workers will attempt to connect back

        uid : str
             string unique identifier

        cores_per_worker : float
             cores to be assigned to each worker. Oversubscription is possible
             by setting cores_per_worker < 1.0. Default=1

        max_workers : int
             caps the maximum number of workers that can be launched.
             default: infinity

        heartbeat_threshold : int
             Seconds since the last message from the interchange after which the
             interchange is assumed to be un-available, and the manager initiates
             shutdown. Default:120s

             Number of seconds since the last message from the interchange after which
             the worker assumes that the interchange is lost and the manager shuts down.
             Default:120

        heartbeat_period : int
             Number of seconds after which a heartbeat message is sent to the
             interchange

        internal_worker_port_range : tuple(int, int)
             Port range from which the port(s) for the workers to connect to the manager
             is picked.
             Default: (50000,60000)

        worker_mode : str
             Pick between 3 supported modes for the worker:
              1. no_container : Worker launched without containers
              2. singularity_reuse : Worker launched inside a singularity container that
                                     will be reused
              3. singularity_single_use : Each worker and task runs inside a new
                                          container instance.

        container_cmd_options: str
              Container command strings to be added to associated container command.
              For example, singularity exec {container_cmd_options}

        scheduler_mode : str
             Pick between 2 supported modes for the manager:
              1. hard: the manager cannot change the launched container type
              2. soft: the manager can decide whether to launch different containers

        worker_type : str
             If set, the worker type for this manager is fixed. Default: None

        poll_period : int
             Timeout period used by the manager in milliseconds. Default: 10ms
        """

        global logger
        # This is expected to be used only in unit test
        if logger is None:
            logger = set_file_logger(
                os.path.join(logdir, uid, "manager.log"),
                name="funcx_manager",
                level=logging.DEBUG,
            )

        logger.info("Manager started")

        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.DEALER)
        self.task_incoming.setsockopt(zmq.IDENTITY, uid.encode("utf-8"))
        # Linger is set to 0, so that the manager can exit even when there might be
        # messages in the pipe
        self.task_incoming.setsockopt(zmq.LINGER, 0)
        self.task_incoming.connect(task_q_url)

        self.logdir = logdir
        self.debug = debug
        self.block_id = block_id
        self.result_outgoing = self.context.socket(zmq.DEALER)
        self.result_outgoing.setsockopt(zmq.IDENTITY, uid.encode("utf-8"))
        self.result_outgoing.setsockopt(zmq.LINGER, 0)
        self.result_outgoing.connect(result_q_url)

        logger.info("Manager connected")

        self.uid = uid

        self.worker_mode = worker_mode
        self.container_cmd_options = container_cmd_options
        self.scheduler_mode = scheduler_mode
        self.worker_type = worker_type
        self.worker_max_idletime = worker_max_idletime
        self.cores_on_node = multiprocessing.cpu_count()
        self.max_workers = max_workers
        self.cores_per_workers = cores_per_worker
        self.available_mem_on_node = round(
            psutil.virtual_memory().available / (2 ** 30), 1
        )
        self.max_worker_count = min(
            max_workers, math.floor(self.cores_on_node / cores_per_worker)
        )
        self.worker_map = WorkerMap(self.max_worker_count)

        self.internal_worker_port_range = internal_worker_port_range

        self.funcx_task_socket = self.context.socket(zmq.ROUTER)
        self.funcx_task_socket.set_hwm(0)
        self.address = "127.0.0.1"
        self.worker_port = self.funcx_task_socket.bind_to_random_port(
            "tcp://*",
            min_port=self.internal_worker_port_range[0],
            max_port=self.internal_worker_port_range[1],
        )

        logger.info(
            "Manager listening on {} port for incoming worker connections".format(
                self.worker_port
            )
        )

        self.task_queues = {}
        if worker_type:
            self.task_queues[worker_type] = queue.Queue()
        self.outstanding_task_count = {}
        self.task_type_mapping = {}

        self.pending_result_queue = mpQueue()

        self.max_queue_size = max_queue_size + self.max_worker_count
        self.tasks_per_round = 1

        self.heartbeat_period = heartbeat_period
        self.heartbeat_threshold = heartbeat_threshold
        self.poll_period = poll_period
        self.serializer = FuncXSerializer()
        self.next_worker_q = []  # FIFO queue for spinning up workers.
        self.worker_procs = {}

        self.task_status_deltas = {}

        self._kill_event = threading.Event()
        self._result_pusher_thread = threading.Thread(
            target=self.push_results, args=(self._kill_event,)
        )
        self._status_report_thread = threading.Thread(
            target=self._status_report_loop, args=(self._kill_event,)
        )
        self.container_switch_count = 0

        self.poller = zmq.Poller()
        self.poller.register(self.task_incoming, zmq.POLLIN)
        self.poller.register(self.funcx_task_socket, zmq.POLLIN)

        self.task_done_counter = 0

        self.local_data_path = local_data_path
        self.globus_ep_id = globus_ep_id
        self.rsync_ip = rsync_ip
        self.rsync_username = rsync_username
        self.check_rsync_auth = check_rsync_auth
        os.environ["LOCAL_PATH"] = self.local_data_path
        os.environ["GLOBUS_EP_ID"] = self.globus_ep_id
        os.environ["RSYNC_IP"] = self.rsync_ip
        os.environ["RSYNC_USERNAME"] = self.rsync_username
        os.environ["CHECK_RSYNC_AUTH"] = self.check_rsync_auth
        self.performance_info = {}
        self.performance_info['cpu_cores'] = psutil.cpu_count(logical=True)
        self.performance_info['cpu_freqs_max'] = psutil.cpu_freq().max
        self.performance_info['cpu_freqs_min'] = psutil.cpu_freq().min

    def create_reg_message(self):
        """Creates a registration message to identify the worker to the interchange"""
        msg = {
            "parsl_v": PARSL_VERSION,
            "python_v": "{}.{}.{}".format(
                sys.version_info.major, sys.version_info.minor, sys.version_info.micro
            ),
            "max_worker_count": self.max_worker_count,
            "cores": self.cores_on_node,
            "mem": self.available_mem_on_node,
            "block_id": self.block_id,
            "worker_type": self.worker_type,
            "os": platform.system(),
            "hname": platform.node(),
            "dir": os.getcwd(),
            "cpu_cores": self.performance_info['cpu_cores'],
            "cpu_freqs_max": self.performance_info['cpu_freqs_max'],
        }
        b_msg = json.dumps(msg).encode("utf-8")
        return b_msg

    def pull_tasks(self, kill_event):
        """Pull tasks from the incoming tasks 0mq pipe onto the internal
        pending task queue


        While :
            receive results and task requests from the workers
            receive tasks/heartbeats from the Interchange
            match tasks to workers
            if task doesn't have appropriate worker type:
                 launch worker of type.. with LRU or some sort of caching strategy.
            if workers >> tasks:
                 advertize available capacity

        Parameters:
        -----------
        kill_event : threading.Event
              Event to let the thread know when it is time to die.
        """
        logger.info("[TASK PULL THREAD] starting")

        # Send a registration message
        msg = self.create_reg_message()
        logger.debug(f"Sending registration message: {msg}")
        self.task_incoming.send(msg)
        last_interchange_contact = time.time()
        task_recv_counter = 0

        poll_timer = self.poll_period

        new_worker_map = None
        while not kill_event.is_set():
            # Disabling the check on ready_worker_queue disables batching
            logger.debug("[TASK_PULL_THREAD] Loop start")
            pending_task_count = task_recv_counter - self.task_done_counter
            ready_worker_count = self.worker_map.ready_worker_count()
            logger.debug(
                "[TASK_PULL_THREAD pending_task_count: %s, Ready_worker_count: %s",
                pending_task_count,
                ready_worker_count,
            )

            if pending_task_count < self.max_queue_size and ready_worker_count > 0:
                ads = self.worker_map.advertisement()
                logger.debug(f"[TASK_PULL_THREAD] Requesting tasks: {ads}")
                msg = pickle.dumps(ads)
                self.task_incoming.send(msg)

            # Receive results from the workers, if any
            socks = dict(self.poller.poll(timeout=poll_timer))

            if (
                self.funcx_task_socket in socks
                and socks[self.funcx_task_socket] == zmq.POLLIN
            ):
                self.poll_funcx_task_socket()

            # Receive task batches from Interchange and forward to workers
            if self.task_incoming in socks and socks[self.task_incoming] == zmq.POLLIN:

                # If we want to wrap the task_incoming polling into a separate function,
                # we need to
                #   self.poll_task_incoming(
                #       poll_timer,
                #       last_interchange_contact,
                #       kill_event,
                #       task_revc_counter
                #   )
                poll_timer = 0
                _, pkl_msg = self.task_incoming.recv_multipart()
                message = pickle.loads(pkl_msg)
                last_interchange_contact = time.time()

                if message == "STOP":
                    logger.critical("[TASK_PULL_THREAD] Received stop request")
                    kill_event.set()
                    break

                elif message == HEARTBEAT_CODE:
                    logger.debug("Got heartbeat from interchange")

                else:
                    tasks = [
                        (rt["local_container"], Message.unpack(rt["raw_buffer"]))
                        for rt in message
                    ]

                    task_recv_counter += len(tasks)
                    logger.debug(
                        "[TASK_PULL_THREAD] Got tasks: {} of {}".format(
                            [t[1].task_id for t in tasks], task_recv_counter
                        )
                    )

                    for task_type, task in tasks:
                        logger.debug(f"[TASK DEBUG] Task is of type: {task_type}")

                        if task_type not in self.task_queues:
                            self.task_queues[task_type] = queue.Queue()
                        if task_type not in self.outstanding_task_count:
                            self.outstanding_task_count[task_type] = 0
                        self.task_queues[task_type].put(task)
                        self.outstanding_task_count[task_type] += 1
                        self.task_type_mapping[task.task_id] = task_type
                        logger.debug(
                            "Got task: Outstanding task counts: {}".format(
                                self.outstanding_task_count
                            )
                        )
                        logger.debug(f"Task {task} pushed to a task queue {task_type}")

            else:
                logger.debug("[TASK_PULL_THREAD] No incoming tasks")
                # Limit poll duration to heartbeat_period
                # heartbeat_period is in s vs poll_timer in ms
                if not poll_timer:
                    poll_timer = self.poll_period
                poll_timer = min(self.heartbeat_period * 1000, poll_timer * 2)

                # Only check if no messages were received.
                if time.time() > last_interchange_contact + self.heartbeat_threshold:
                    logger.critical(
                        "[TASK_PULL_THREAD] Missing contact with interchange beyond "
                        "heartbeat_threshold"
                    )
                    kill_event.set()
                    logger.critical("Killing all workers")
                    for proc in self.worker_procs.values():
                        proc.kill()
                    logger.critical("[TASK_PULL_THREAD] Exiting")
                    break

            logger.debug(f"To-Die Counts: {self.worker_map.to_die_count}")
            logger.debug(
                "Alive worker counts: {}".format(
                    self.worker_map.total_worker_type_counts
                )
            )

            new_worker_map = naive_scheduler(
                self.task_queues,
                self.outstanding_task_count,
                self.max_worker_count,
                new_worker_map,
                self.worker_map.to_die_count,
                logger=logger,
            )
            logger.debug(f"[SCHEDULER] New worker map: {new_worker_map}")

            # NOTE: Wipes the queue -- previous scheduling loops don't affect what's
            # needed now.
            self.next_worker_q, need_more = self.worker_map.get_next_worker_q(
                new_worker_map
            )

            # Spin up any new workers according to the worker queue.
            # Returns the total number of containers that have spun up.
            self.worker_procs.update(
                self.worker_map.spin_up_workers(
                    self.next_worker_q,
                    mode=self.worker_mode,
                    debug=self.debug,
                    container_cmd_options=self.container_cmd_options,
                    address=self.address,
                    uid=self.uid,
                    logdir=self.logdir,
                    worker_port=self.worker_port,
                )
            )
            logger.debug(f"[SPIN UP] Worker processes: {self.worker_procs}")

            #  Count the workers of each type that need to be removed
            spin_downs, container_switch_count = self.worker_map.spin_down_workers(
                new_worker_map,
                worker_max_idletime=self.worker_max_idletime,
                need_more=need_more,
                scheduler_mode=self.scheduler_mode,
            )
            self.container_switch_count += container_switch_count
            logger.debug(
                "Container switch count: total {}, cur {}".format(
                    self.container_switch_count, container_switch_count
                )
            )

            for w_type in spin_downs:
                self.remove_worker_init(w_type)

            current_worker_map = self.worker_map.get_worker_counts()
            for task_type in current_worker_map:
                if task_type == "unused":
                    continue

                # *** Match tasks to workers *** #
                else:
                    available_workers = current_worker_map[task_type]
                    logger.debug(
                        "Available workers of type {}: {}".format(
                            task_type, available_workers
                        )
                    )

                    for _i in range(available_workers):
                        if (
                            task_type in self.task_queues
                            and not self.task_queues[task_type].qsize() == 0
                            and not self.worker_map.worker_queues[task_type].qsize()
                            == 0
                        ):

                            logger.debug(
                                "Task type {} has task queue size {}".format(
                                    task_type, self.task_queues[task_type].qsize()
                                )
                            )
                            logger.debug(
                                "... and available workers: {}".format(
                                    self.worker_map.worker_queues[task_type].qsize()
                                )
                            )

                            self.send_task_to_worker(task_type)

    def poll_funcx_task_socket(self, test=False):
        try:
            w_id, m_type, message = self.funcx_task_socket.recv_multipart()
            if m_type == b"REGISTER":
                reg_info = pickle.loads(message)
                logger.debug(f"Registration received from worker:{w_id} {reg_info}")
                self.worker_map.register_worker(w_id, reg_info["worker_type"])

            elif m_type == b"TASK_RET":
                logger.debug(f"Result received from worker: {w_id}")
                logger.debug(f"[TASK_PULL_THREAD] Got result: {message}")
                self.pending_result_queue.put(message)
                self.worker_map.put_worker(w_id)
                self.task_done_counter += 1
                task_id = pickle.loads(message)["task_id"]
                task_type = self.task_type_mapping.pop(task_id)
                self.task_status_deltas.pop(task_id, None)
                logger.debug(f"Task type: {task_type}")
                self.outstanding_task_count[task_type] -= 1
                logger.debug(
                    "Got result: Outstanding task counts: {}".format(
                        self.outstanding_task_count
                    )
                )

            elif m_type == b"WRKR_DIE":
                logger.debug(
                    f"[WORKER_REMOVE] Removing worker {w_id} from worker_map..."
                )
                logger.debug(
                    "Ready worker counts: {}".format(
                        self.worker_map.ready_worker_type_counts
                    )
                )
                logger.debug(
                    "Total worker counts: {}".format(
                        self.worker_map.total_worker_type_counts
                    )
                )
                self.worker_map.remove_worker(w_id)
                proc = self.worker_procs.pop(w_id.decode())
                if not proc.poll():
                    try:
                        proc.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        logger.warning(
                            "[WORKER_REMOVE] Timeout waiting for worker %s process to "
                            "terminate",
                            w_id,
                        )
                logger.debug(f"[WORKER_REMOVE] Removing worker {w_id} process object")
                logger.debug(f"[WORKER_REMOVE] Worker processes: {self.worker_procs}")

            if test:
                return pickle.loads(message)

        except Exception as e:
            logger.exception(f"[TASK_PULL_THREAD] FUNCX : caught {e}")

    def send_task_to_worker(self, task_type):
        task = self.task_queues[task_type].get()
        worker_id = self.worker_map.get_worker(task_type)

        logger.debug(f"Sending task {task.task_id} to {worker_id}")
        # TODO: Some duplication of work could be avoided here
        to_send = [
            worker_id,
            pickle.dumps(task.task_id),
            pickle.dumps(task.container_id),
            task.pack(),
        ]
        self.funcx_task_socket.send_multipart(to_send)
        self.worker_map.update_worker_idle(task_type)
        if task.task_id != "KILL":
            logger.debug(f"Set task {task.task_id} to RUNNING")
            self.task_status_deltas[task.task_id] = TaskStatusCode.RUNNING
        logger.debug("Sending complete!")

    def _status_report_loop(self, kill_event):
        logger.debug("[STATUS] Manager status reporting loop starting")

        while not kill_event.is_set():
            msg = ManagerStatusReport(
                self.task_status_deltas,
                self.container_switch_count,
            )
            logger.info(
                f"[STATUS] Sending status report to interchange: {msg.task_statuses}"
            )
            self.pending_result_queue.put(msg)
            logger.info("[STATUS] Clearing task deltas")
            self.task_status_deltas.clear()
            time.sleep(self.heartbeat_period)

    def push_results(self, kill_event, max_result_batch_size=1):
        """Listens on the pending_result_queue and sends out results via 0mq

        Parameters:
        -----------
        kill_event : threading.Event
              Event to let the thread know when it is time to die.
        """

        logger.debug("[RESULT_PUSH_THREAD] Starting thread")

        push_poll_period = (
            max(10, self.poll_period) / 1000
        )  # push_poll_period must be atleast 10 ms
        logger.debug(f"[RESULT_PUSH_THREAD] push poll period: {push_poll_period}")

        last_beat = time.time()
        items = []

        while not kill_event.is_set():
            try:
                r = self.pending_result_queue.get(block=True, timeout=push_poll_period)
                # This avoids the interchange searching and attempting to unpack every
                # message in case it's a status report.
                # (It would be better to use Task Messages eventually to make this more
                #  uniform)
                # TODO: use task messages, and don't have to prepend
                if isinstance(r, ManagerStatusReport):
                    items.insert(0, r.pack())
                else:
                    items.append(r)
            except queue.Empty:
                pass
            except Exception as e:
                logger.exception(f"[RESULT_PUSH_THREAD] Got an exception: {e}")

            # If we have reached poll_period duration or timer has expired, we send
            # results
            if (
                len(items) >= self.max_queue_size
                or time.time() > last_beat + push_poll_period
            ):
                last_beat = time.time()
                if items:
                    self.result_outgoing.send_multipart(items)
                    items = []

        logger.critical("[RESULT_PUSH_THREAD] Exiting")

    def remove_worker_init(self, worker_type):
        """
        Kill/Remove a worker of a given worker_type.

        Add a kill message to the task_type queue.

        Assumption : All workers of the same type are uniform, and therefore don't
                     discriminate when killing.
        """

        logger.debug(
            "[WORKER_REMOVE] Appending KILL message to worker queue {}".format(
                worker_type
            )
        )
        self.worker_map.to_die_count[worker_type] += 1
        task = Task(task_id="KILL", container_id="RAW", task_buffer="KILL")
        self.task_queues[worker_type].put(task)

    def start(self):
        """
        * while True:
            Receive tasks and start appropriate workers
            Push tasks to available workers
            Forward results
        """

        if self.worker_type and self.scheduler_mode == "hard":
            logger.debug(
                "[MANAGER] Start an initial worker with worker type {}".format(
                    self.worker_type
                )
            )
            self.worker_procs.update(
                self.worker_map.add_worker(
                    worker_id=str(self.worker_map.worker_id_counter),
                    worker_type=self.worker_type,
                    container_cmd_options=self.container_cmd_options,
                    address=self.address,
                    debug=self.debug,
                    uid=self.uid,
                    logdir=self.logdir,
                    worker_port=self.worker_port,
                )
            )

        logger.debug("Initial workers launched")
        self._result_pusher_thread.start()
        self._status_report_thread.start()
        self.pull_tasks(self._kill_event)
        logger.info("Waiting")


def cli_run():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--debug", action="store_true", help="Count of apps to launch"
    )
    parser.add_argument(
        "-l",
        "--logdir",
        default="process_worker_pool_logs",
        help="Process worker pool log directory",
    )
    parser.add_argument(
        "-u",
        "--uid",
        default=str(uuid.uuid4()).split("-")[-1],
        help="Unique identifier string for Manager",
    )
    parser.add_argument(
        "-b", "--block_id", default=None, help="Block identifier string for Manager"
    )
    parser.add_argument(
        "-c",
        "--cores_per_worker",
        default="1.0",
        help="Number of cores assigned to each worker process. Default=1.0",
    )
    parser.add_argument(
        "-t", "--task_url", required=True, help="REQUIRED: ZMQ url for receiving tasks"
    )
    parser.add_argument(
        "--max_workers",
        default=float("inf"),
        help="Caps the maximum workers that can be launched, default:infinity",
    )
    parser.add_argument(
        "--hb_period",
        default=30,
        help="Heartbeat period in seconds. Uses manager default unless set",
    )
    parser.add_argument(
        "--hb_threshold",
        default=120,
        help="Heartbeat threshold in seconds. Uses manager default unless set",
    )
    parser.add_argument("--poll", default=10, help="Poll period used in milliseconds")
    parser.add_argument(
        "--worker_type", default=None, help="Fixed worker type of manager"
    )
    parser.add_argument(
        "--worker_mode",
        default="singularity_reuse",
        help=(
            "Choose the mode of operation from "
            "(no_container, singularity_reuse, singularity_single_use"
        ),
    )
    parser.add_argument(
        "--container_cmd_options",
        default="",
        help=("Container cmd options to add to container startup cmd"),
    )
    parser.add_argument(
        "--scheduler_mode",
        default="soft",
        help=("Choose the mode of scheduler " "(hard, soft"),
    )
    parser.add_argument(
        "-r",
        "--result_url",
        required=True,
        help="REQUIRED: ZMQ url for posting results",
    )
    parser.add_argument(
        "--log_max_bytes",
        default=256 * 1024 * 1024,
        help="The maximum bytes per logger file in bytes",
    )
    parser.add_argument(
        "--log_backup_count",
        default=1,
        help="The number of backup (must be non-zero) per logger file",
    )

    parser.add_argument(
        "--local_data_path", default="", help="For setting globus ep path"
    )

    parser.add_argument(
        "--globus_ep_id", default="", help="For setting globus ep id"
    )

    parser.add_argument(
        "--rsync_ip", default="", help="For setting rsync ip"
    )

    parser.add_argument(
       "--rsync_username", default="", help="For setting rsync_username"
    )

    parser.add_argument(
        "--check_rsync_auth", default="False", help="For setting check_rsync_auth"
    )

    args = parser.parse_args()

    try:
        os.makedirs(os.path.join(args.logdir, args.uid))
    except FileExistsError:
        pass

    try:
        global logger
        # TODO The config options for the rotatingfilehandler need to be implemented
        # and checked so that it is user configurable
        logger = set_file_logger(
            os.path.join(args.logdir, args.uid, "manager.log"),
            name="funcx_manager",
            level=logging.DEBUG if args.debug is True else logging.INFO,
            max_bytes=float(
                args.log_max_bytes
            ),  # TODO: Test if this still works on forwarder_rearch_1
            backup_count=int(args.log_backup_count),
        )  # TODO: Test if this still works on forwarder_rearch_1

        logger.info(f"Python version: {sys.version}")
        logger.info(f"Debug logging: {args.debug}")
        logger.info(f"Log dir: {args.logdir}")
        logger.info(f"Manager ID: {args.uid}")
        logger.info(f"Block ID: {args.block_id}")
        logger.info(f"cores_per_worker: {args.cores_per_worker}")
        logger.info(f"task_url: {args.task_url}")
        logger.info(f"local_path: {args.local_data_path}")
        logger.info(f"globus_ep_id: {args.globus_ep_id}")
        logger.info(f"rsync_ip: {args.rsync_ip}")
        logger.info(f"rsync_username: {args.rsync_username}")
        logger.info(f"check_rsync_auth: {args.check_rsync_auth}")
        logger.info(f"result_url: {args.result_url}")
        logger.info(f"hb_period: {args.hb_period}")
        logger.info(f"hb_threshold: {args.hb_threshold}")
        logger.info(f"max_workers: {args.max_workers}")
        logger.info(f"poll_period: {args.poll}")
        logger.info(f"worker_mode: {args.worker_mode}")
        logger.info(f"container_cmd_options: {args.container_cmd_options}")
        logger.info(f"scheduler_mode: {args.scheduler_mode}")
        logger.info(f"worker_type: {args.worker_type}")
        logger.info(f"log_max_bytes: {args.log_max_bytes}")
        logger.info(f"log_backup_count: {args.log_backup_count}")

        manager = Manager(
            task_q_url=args.task_url,
            result_q_url=args.result_url,
            uid=args.uid,
            block_id=args.block_id,
            cores_per_worker=float(args.cores_per_worker),
            max_workers=args.max_workers
            if args.max_workers == float("inf")
            else int(args.max_workers),
            heartbeat_threshold=int(args.hb_threshold),
            heartbeat_period=int(args.hb_period),
            logdir=args.logdir,
            debug=args.debug,
            worker_mode=args.worker_mode,
            container_cmd_options=args.container_cmd_options,
            scheduler_mode=args.scheduler_mode,
            worker_type=args.worker_type,
            poll_period=int(args.poll),
            local_data_path=args.local_data_path,
            globus_ep_id=args.globus_ep_id,
            rsync_ip=args.rsync_ip,
            rsync_username=args.rsync_username,
            check_rsync_auth=args.check_rsync_auth
        )
        manager.start()

    except Exception as e:
        logger.critical("process_worker_pool exiting from an exception")
        logger.exception(f"Caught error: {e}")
        raise
    else:
        logger.info("process_worker_pool main event loop exiting normally")
        print("PROCESS_WORKER_POOL main event loop exiting normally")


if __name__ == "__main__":
    cli_run()
