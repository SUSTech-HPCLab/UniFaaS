"""HighThroughputExecutor builds on the Swift/T EMEWS architecture to use MPI for fast
task distribution

There's a slow but sure deviation from Parsl's Executor interface here, that needs
to be addressed.
"""

import logging
import os
import pickle
import queue
import threading
import time
from concurrent.futures import Future
from multiprocessing import Process

import daemon
from parsl.dataflow.error import ConfigurationError
from parsl.executors.errors import BadMessage, ScalingFailed

# from parsl.executors.base import ParslExecutor
from parsl.executors.status_handling import BlockProviderExecutor
from parsl.providers import LocalProvider
from parsl.utils import RepresentationMixin
from typing import Union

from funcx import set_file_logger
from funcx.serialize import FuncXSerializer

# from parsl.executors.high_throughput import interchange
from funcx_endpoint.executors.high_throughput import interchange, zmq_pipes
from funcx_endpoint.executors.high_throughput.mac_safe_queue import mpQueue
from funcx_endpoint.executors.high_throughput.messages import (
    EPStatusReport,
    Heartbeat,
    HeartbeatReq,
    Task,
)
from funcx_endpoint.strategies.simple import SimpleStrategy
from funcx_endpoint.executors.high_throughput.local_monitor import LocalMonitor

fx_serializer = FuncXSerializer()

# TODO: YADU There's a bug here which causes some of the log messages to write out to
# stderr
# "logging" python3 self.stream.flush() OSError: [Errno 9] Bad file descriptor

logger = logging.getLogger(__name__)
# if not logger.hasHandlers():
#    logger = set_file_logger("executor.log", name=__name__)


BUFFER_THRESHOLD = 1024 * 1024
ITEM_THRESHOLD = 1024


class HighThroughputExecutor(BlockProviderExecutor, RepresentationMixin):
    """Executor designed for cluster-scale

    The HighThroughputExecutor system has the following components:
      1. The HighThroughputExecutor instance which is run as part of the Parsl script.
      2. The Interchange which is acts as a load-balancing proxy between workers and
         Parsl
      3. The multiprocessing based worker pool which coordinates task execution over
         several cores on a node.
      4. ZeroMQ pipes connect the HighThroughputExecutor, Interchange and the
         process_worker_pool

    Here is a diagram

    .. code:: python


                        |  Data   |  Executor   |  Interchange  | External Process(es)
                        |  Flow   |             |               |
                   Task | Kernel  |             |               |
                 +----->|-------->|------------>|->outgoing_q---|-> process_worker_pool
                 |      |         |             | batching      |    |         |
           Parsl<---Fut-|         |             | load-balancing|  result   exception
                     ^  |         |             | watchdogs     |    |         |
                     |  |         |   Q_mngmnt  |               |    V         V
                     |  |         |    Thread<--|-incoming_q<---|--- +---------+
                     |  |         |      |      |               |
                     |  |         |      |      |               |
                     +----update_fut-----+


    Parameters
    ----------

    provider : :class:`~parsl.providers.provider_base.ExecutionProvider`
       Provider to access computation resources. Can be one of
       :class:`~parsl.providers.aws.aws.EC2Provider`,
        :class:`~parsl.providers.cobalt.cobalt.Cobalt`,
        :class:`~parsl.providers.condor.condor.Condor`,
        :class:`~parsl.providers.googlecloud.googlecloud.GoogleCloud`,
        :class:`~parsl.providers.gridEngine.gridEngine.GridEngine`,
        :class:`~parsl.providers.jetstream.jetstream.Jetstream`,
        :class:`~parsl.providers.local.local.Local`,
        :class:`~parsl.providers.sge.sge.GridEngine`,
        :class:`~parsl.providers.slurm.slurm.Slurm`, or
        :class:`~parsl.providers.torque.torque.Torque`.

    label : str
        Label for this executor instance.

    launch_cmd : str
        Command line string to launch the process_worker_pool from the provider. The
        command line string will be formatted with appropriate values for the following
        values: (
            debug,
            task_url,
            result_url,
            cores_per_worker,
            nodes_per_block,
            heartbeat_period,
            heartbeat_threshold,
            logdir,
        ).
        For example:
        launch_cmd="process_worker_pool.py {debug} -c {cores_per_worker} \
        --task_url={task_url} --result_url={result_url}"

    address : string
        An address of the host on which the executor runs, which is reachable from the
        network in which workers will be running. This can be either a hostname as
        returned by `hostname` or an IP address. Most login nodes on clusters have
        several network interfaces available, only some of which can be reached
        from the compute nodes. Some trial and error might be necessary to
        indentify what addresses are reachable from compute nodes.

    worker_ports : (int, int)
        Specify the ports to be used by workers to connect to Parsl. If this
        option is specified, worker_port_range will not be honored.

    worker_port_range : (int, int)
        Worker ports will be chosen between the two integers provided.

    interchange_port_range : (int, int)
        Port range used by Parsl to communicate with the Interchange.

    working_dir : str
        Working dir to be used by the executor.

    worker_debug : Bool
        Enables worker debug logging.

    managed : Bool
        If this executor is managed by the DFK or externally handled.

    cores_per_worker : float
        cores to be assigned to each worker. Oversubscription is possible
        by setting cores_per_worker < 1.0. Default=1

    mem_per_worker : float
        Memory to be assigned to each worker. Default=None(no limits)

    max_workers_per_node : int
        Caps the number of workers launched by the manager. Default: infinity

    suppress_failure : Bool
        If set, the interchange will suppress failures rather than terminate early.
        Default: False

    heartbeat_threshold : int
        Seconds since the last message from the counterpart in the communication pair:
        (interchange, manager) after which the counterpart is assumed to be unavailable.
        Default:120s

    heartbeat_period : int
        Number of seconds after which a heartbeat message indicating liveness is sent to
        the endpoint
        counterpart (interchange, manager). Default:30s

    poll_period : int
        Timeout period to be used by the executor components in milliseconds.
        Increasing poll_periods trades performance for cpu efficiency. Default: 10ms

    container_image : str
        Path or identfier to the container image to be used by the workers

    scheduler_mode: str
        Scheduling mode to be used by the node manager. Options: 'hard', 'soft'
        'hard' -> managers cannot replace worker's container types
        'soft' -> managers can replace unused worker's containers based on demand

    worker_mode : str
        Select the mode of operation from no_container, singularity_reuse,
        singularity_single_use
        Default: singularity_reuse

    container_cmd_options: str
        Container command strings to be added to associated container command.
        For example, singularity exec {container_cmd_options}

    task_status_queue : queue.Queue
        Queue to pass updates to task statuses back to the forwarder.

    strategy: Stategy Object
        Specify the scaling strategy to use for this executor.

    launch_cmd: str
        Specify the launch command as using f-string format that will be used to specify
        command to launch managers. Default: None

    prefetch_capacity: int
        Number of tasks that can be fetched by managers in excess of available
        workers is a prefetching optimization. This option can cause poor
        load-balancing for long running functions.
        Default: 10

    provider: Provider object
        Provider determines how managers can be provisioned, say LocalProvider
        offers forked processes, and SlurmProvider interfaces to request
        resources from the Slurm batch scheduler.
        Default: LocalProvider

    funcx_service_address: str
        Override funcx_service_address used by the FuncXClient. If no address
        is specified, the FuncXClient's default funcx_service_address is used.
        Default: None
    """

    def __init__(
            self,
            label="HighThroughputExecutor",
            # NEW
            strategy=SimpleStrategy(),
            max_workers_per_node=float("inf"),
            mem_per_worker=None,
            launch_cmd=None,
            # Container specific
            worker_mode="no_container",
            scheduler_mode="hard",
            container_type=None,
            container_cmd_options="",
            cold_routing_interval=10.0,
            # Tuning info
            prefetch_capacity=10,
            provider=LocalProvider(),
            address="127.0.0.1",
            worker_ports=None,
            worker_port_range=(54000, 55000),
            interchange_port_range=(55000, 56000),
            storage_access=None,
            working_dir=None,
            worker_debug=False,
            cores_per_worker=1.0,
            heartbeat_threshold=120,
            heartbeat_period=30,
            poll_period=10,
            container_image=None,
            suppress_failure=False,
            run_dir=None,
            endpoint_id=None,
            managed=True,
            interchange_local=True,
            passthrough=True,
            funcx_service_address=None,
            task_status_queue=None,
            globus_ep_id=None,
            rsync_ip=None,
            rsync_username=None,
            rsync_password_file=None,
            check_rsync_auth=False,
            local_data_path=None,
            globus_polling_interval=10,
            monitor=LocalMonitor(),
            transfer_command_server_config=None,
            force_start_config=None,
            ic_total_workers=0,
            transfer_method="sftp",
    ):
        # These attributes are used by the interchange
        self.globus_ep_id = globus_ep_id
        self.rsync_ip = rsync_ip
        self.rsync_username = rsync_username
        self.rsync_password_file = rsync_password_file
        self.transfer_command_server_config = transfer_command_server_config
        self.force_start_config = force_start_config
        self.ic_total_workers = ic_total_workers
        self.check_rsync_auth = check_rsync_auth
        self.local_data_path = local_data_path
        self.transfer_method = transfer_method
        self.globus_polling_interval = globus_polling_interval
        logger.debug("Initializing HighThroughputExecutor")
        BlockProviderExecutor.__init__(self, provider)

        self.label = label
        self.launch_cmd = launch_cmd
        self.worker_debug = worker_debug
        self.max_workers_per_node = max_workers_per_node

        # NEW
        self.strategy = strategy
        self.cores_per_worker = cores_per_worker
        self.mem_per_worker = mem_per_worker

        # Container specific
        self.scheduler_mode = scheduler_mode
        self.container_type = container_type
        self.container_cmd_options = container_cmd_options
        self.cold_routing_interval = cold_routing_interval

        # Tuning info
        self.prefetch_capacity = prefetch_capacity

        self.storage_access = storage_access if storage_access is not None else []
        if len(self.storage_access) > 1:
            raise ConfigurationError(
                "Multiple storage access schemes are not supported"
            )
        self.working_dir = working_dir
        self.managed = managed
        self.blocks = []
        self.cores_per_worker = cores_per_worker
        self.endpoint_id = endpoint_id
        self._task_counter = 0
        self.address = address
        self.worker_ports = worker_ports
        self.worker_port_range = worker_port_range
        self.interchange_port_range = interchange_port_range
        self.heartbeat_threshold = heartbeat_threshold
        self.heartbeat_period = heartbeat_period
        self.poll_period = poll_period
        self.suppress_failure = suppress_failure
        self.run_dir = run_dir
        self.queue_proc = None
        self.interchange_local = interchange_local
        self.passthrough = passthrough
        self.task_status_queue = task_status_queue

        # FuncX specific options
        self.funcx_service_address = funcx_service_address
        self.container_image = container_image
        self.worker_mode = worker_mode
        self.last_response_time = time.time()
        self.monitor = monitor

        if not launch_cmd:
            self.launch_cmd = (
                "process_worker_pool.py {debug} {max_workers} "
                "-c {cores_per_worker} "
                "--poll {poll_period} "
                "--task_url={task_url} "
                "--result_url={result_url} "
                "--logdir={logdir} "
                "--hb_period={heartbeat_period} "
                "--hb_threshold={heartbeat_threshold} "
                "--mode={worker_mode} "
                "--container_image={container_image} "
            )

        self.ix_launch_cmd = (
            "funcx-interchange {debug} -c={client_address} "
            "--client_ports={client_ports} "
            "--worker_port_range={worker_port_range} "
            "--logdir={logdir} "
            "{suppress_failure} "
        )

    def initialize_scaling(self):
        """Compose the launch command and call the scale_out

        This should be implemented in the child classes to take care of
        executor specific oddities.
        """
        debug_opts = "--debug" if self.worker_debug else ""
        max_workers = (
            ""
            if self.max_workers == float("inf")
            else f"--max_workers={self.max_workers}"
        )

        l_cmd = self.launch_cmd.format(
            debug=debug_opts,
            task_url=self.worker_task_url,
            result_url=self.worker_result_url,
            cores_per_worker=self.cores_per_worker,
            max_workers=max_workers,
            nodes_per_block=self.provider.nodes_per_block,
            heartbeat_period=self.heartbeat_period,
            heartbeat_threshold=self.heartbeat_threshold,
            poll_period=self.poll_period,
            logdir=os.path.join(self.run_dir, self.label),
            worker_mode=self.worker_mode,
            container_image=self.container_image,
        )
        self.launch_cmd = l_cmd
        logger.debug(f"Launch command: {self.launch_cmd}")

        self._scaling_enabled = self.provider.scaling_enabled
        logger.debug(
            "Starting HighThroughputExecutor with provider:\n%s", self.provider
        )
        if hasattr(self.provider, "init_blocks"):
            try:
                self.scale_out(blocks=self.provider.init_blocks)
            except Exception as e:
                logger.error(f"Scaling out failed: {e}")
                raise e

    def start(self, results_passthrough=None):
        """Create the Interchange process and connect to it."""
        self.outgoing_q = zmq_pipes.TasksOutgoing(
            "0.0.0.0", self.interchange_port_range
        )
        self.incoming_q = zmq_pipes.ResultsIncoming(
            "0.0.0.0", self.interchange_port_range
        )
        self.command_client = zmq_pipes.CommandClient(
            "0.0.0.0", self.interchange_port_range
        )

        self.is_alive = True

        if self.passthrough is True:
            if results_passthrough is None:
                raise Exception(
                    "Executors configured in passthrough mode, must be started with"
                    "a multiprocessing queue for results_passthrough"
                )
            self.results_passthrough = results_passthrough
            logger.debug(f"Executor:{self.label} starting in results_passthrough mode")

        self._executor_bad_state = threading.Event()
        self._executor_exception = None
        self._queue_management_thread = None
        self._start_queue_management_thread()

        if self.interchange_local is True:
            logger.info("Attempting local interchange start")
            self._start_local_interchange_process()
            logger.info(
                "Started local interchange with ports: %s. %s",
                self.worker_task_port,
                self.worker_result_port,
            )

        logger.debug(f"Created management thread: {self._queue_management_thread}")

        if self.provider:
            # self.initialize_scaling()
            pass
        else:
            self._scaling_enabled = False
            logger.debug("Starting HighThroughputExecutor with no provider")

        return (self.outgoing_q.port, self.incoming_q.port, self.command_client.port)

    def _start_local_interchange_process(self):
        """Starts the interchange process locally

        Starts the interchange process locally and uses an internal command queue to
        get the worker task and result ports that the interchange has bound to.
        """
        comm_q = mpQueue(maxsize=10)
        print(f" Starting local interchange with endpoint id: {self.endpoint_id}")
        self.queue_proc = Process(
            target=interchange.starter,
            args=(comm_q,),
            kwargs={
                "client_address": self.address,
                "client_ports": (
                    self.outgoing_q.port,
                    self.incoming_q.port,
                    self.command_client.port,
                ),
                "provider": self.provider,
                "strategy": self.strategy,
                "poll_period": self.poll_period,
                "heartbeat_period": self.heartbeat_period,
                "heartbeat_threshold": self.heartbeat_threshold,
                "working_dir": self.working_dir,
                "worker_debug": self.worker_debug,
                "max_workers_per_node": self.max_workers_per_node,
                "mem_per_worker": self.mem_per_worker,
                "cores_per_worker": self.cores_per_worker,
                "prefetch_capacity": self.prefetch_capacity,
                # "log_max_bytes": self.log_max_bytes,
                # "log_backup_count": self.log_backup_count,
                "scheduler_mode": self.scheduler_mode,
                "worker_mode": self.worker_mode,
                "container_type": self.container_type,
                "container_cmd_options": self.container_cmd_options,
                "cold_routing_interval": self.cold_routing_interval,
                "funcx_service_address": self.funcx_service_address,
                "interchange_address": self.address,
                "worker_ports": self.worker_ports,
                "worker_port_range": self.worker_port_range,
                "logdir": os.path.join(self.run_dir, self.label),
                "suppress_failure": self.suppress_failure,
                "endpoint_id": self.endpoint_id,
                "logging_level": logging.DEBUG if self.worker_debug else logging.INFO,
                "globus_ep_id": self.globus_ep_id,
                "rsync_ip": self.rsync_ip,
                "rsync_username": self.rsync_username,
                "check_rsync_auth": self.check_rsync_auth,
                "rsync_password_file": self.rsync_password_file,
                "local_data_path": self.local_data_path,
                "transfer_method": self.transfer_method,
                "globus_polling_interval": self.globus_polling_interval,
                "monitor": self.monitor,
                "transfer_command_server_config": self.transfer_command_server_config,
                "force_start_config": self.force_start_config,
                "ic_total_workers": self.ic_total_workers,
            },
        )
        self.queue_proc.start()
        try:
            (self.worker_task_port, self.worker_result_port) = comm_q.get(
                block=True, timeout=300
            )
        except queue.Empty:
            # launching sftp server can take a while
            logger.error(
                "Interchange has not completed initialization in 300s. Aborting"
            )
            raise Exception("Interchange failed to start")

        self.worker_task_url = f"tcp://{self.address}:{self.worker_task_port}"
        self.worker_result_url = "tcp://{}:{}".format(
            self.address, self.worker_result_port
        )

    def _start_remote_interchange_process(self):
        """Starts the interchange process locally

        Starts the interchange process remotely via the provider.channel and
        uses the command channel to request worker urls that the interchange
        has selected.
        """
        logger.debug(
            "Attempting Interchange deployment via channel: {}".format(
                self.provider.channel
            )
        )

        debug_opts = "--debug" if self.worker_debug else ""
        suppress_failure = "--suppress_failure" if self.suppress_failure else ""
        logger.debug(f"Before : \n{self.ix_launch_cmd}\n")
        launch_command = self.ix_launch_cmd.format(
            debug=debug_opts,
            client_address=self.address,
            client_ports="{},{},{}".format(
                self.outgoing_q.port, self.incoming_q.port, self.command_client.port
            ),
            worker_port_range="{},{}".format(
                self.worker_port_range[0], self.worker_port_range[1]
            ),
            logdir=os.path.join(
                self.provider.channel.script_dir,
                "runinfo",
                os.path.basename(self.run_dir),
                self.label,
            ),
            suppress_failure=suppress_failure,
        )

        if self.provider.worker_init:
            launch_command = self.provider.worker_init + "\n" + launch_command

        logger.debug(f"Launch command : \n{launch_command}\n")
        return

    def _queue_management_worker(self):
        """Listen to the queue for task status messages and handle them.

        Depending on the message, tasks will be updated with results, exceptions,
        or updates. It expects the following messages:

        .. code:: python

            {
               "task_id" : <task_id>
               "result"  : serialized result object, if task succeeded
               ... more tags could be added later
            }

            {
               "task_id" : <task_id>
               "exception" : serialized exception object, on failure
            }

        We do not support these yet, but they could be added easily.

        .. code:: python

            {
               "task_id" : <task_id>
               "cpu_stat" : <>
               "mem_stat" : <>
               "io_stat"  : <>
               "started"  : tstamp
            }

        The `None` message is a die request.
        """
        logger.debug("[MTHREAD] queue management worker starting")

        while not self._executor_bad_state.is_set():
            try:
                msgs = self.incoming_q.get(timeout=1)
                self.last_response_time = time.time()

            except queue.Empty:
                logger.debug("[MTHREAD] queue empty")
                # Timed out.
                pass

            except OSError as e:
                logger.exception(
                    "[MTHREAD] Caught broken queue with exception code {}: {}".format(
                        e.errno, e
                    )
                )
                return

            except Exception as e:
                logger.exception(f"[MTHREAD] Caught unknown exception: {e}")
                return

            else:

                if msgs is None:
                    logger.debug("[MTHREAD] Got None, exiting")
                    return

                elif isinstance(msgs, EPStatusReport):
                    logger.debug(f"[MTHREAD] Received EPStatusReport {msgs}")
                    if self.passthrough:
                        self.results_passthrough.put(
                            {"task_id": None, "message": pickle.dumps(msgs)}
                        )

                else:
                    logger.debug("[MTHREAD] Unpacking results")
                    for serialized_msg in msgs:
                        try:
                            msg = pickle.loads(serialized_msg)
                            tid = msg["task_id"]
                        except pickle.UnpicklingError:
                            raise BadMessage("Message received could not be unpickled")

                        except Exception:
                            raise BadMessage(
                                "Message received does not contain 'task_id' field"
                            )

                        if tid == -2 and "info" in msg:
                            logger.warning(
                                "[MTHREAD[ Received info response : {}".format(
                                    msg["info"]
                                )
                            )

                        if tid == -1 and "exception" in msg:
                            # TODO: This could be handled better we are
                            # essentially shutting down the client with little
                            # indication to the user.
                            logger.warning(
                                "[MTHREAD] Executor shutting down due to fatal "
                                "exception from interchange"
                            )
                            self._executor_exception = fx_serializer.deserialize(
                                msg["exception"]
                            )
                            logger.exception(
                                "[MTHREAD] Exception: {}".format(
                                    self._executor_exception
                                )
                            )
                            # Set bad state to prevent new tasks from being submitted
                            self._executor_bad_state.set()
                            # We set all current tasks to this exception to make sure
                            # that this is raised in the main context.
                            for task in self.tasks:
                                self.tasks[task].set_exception(self._executor_exception)
                            break

                        if self.passthrough is True:
                            logger.debug(f"[MTHREAD] Pushing results for task:{tid}")
                            # we are only interested in actual task ids here, not
                            # identifiers for other message types
                            sent_task_id = tid if isinstance(tid, str) else None
                            x = self.results_passthrough.put(
                                {"task_id": sent_task_id, "message": serialized_msg}
                            )
                            logger.debug(f"[MTHREAD] task:{tid} ret value: {x}")
                            logger.debug(
                                "[MTHREAD] task:%s items in queue: %s",
                                tid,
                                self.results_passthrough.qsize(),
                            )
                            continue

                        task_fut = self.tasks.pop(tid)

                        if "result" in msg:
                            result = fx_serializer.deserialize(msg["result"])
                            task_fut.set_result(result)
                        elif "exception" in msg:
                            exception = fx_serializer.deserialize(msg["exception"])
                            task_fut.set_result(exception)
                        else:
                            raise BadMessage(
                                "[MTHREAD] Message received is neither result or "
                                "exception"
                            )

            if not self.is_alive:
                break
        logger.info("[MTHREAD] queue management worker finished")

    # When the executor gets lost, the weakref callback will wake up
    # the queue management thread.
    def weakref_cb(self, q=None):
        """We do not use this yet."""
        q.put(None)

    def _start_queue_management_thread(self):
        """Method to start the management thread as a daemon.

        Checks if a thread already exists, then starts it.
        Could be used later as a restart if the management thread dies.
        """
        if self._queue_management_thread is None:
            logger.debug("Starting queue management thread")
            self._queue_management_thread = threading.Thread(
                target=self._queue_management_worker
            )
            self._queue_management_thread.daemon = True
            self._queue_management_thread.start()
            logger.debug("Started queue management thread")

        else:
            logger.debug("Management thread already exists, returning")

    def hold_worker(self, worker_id):
        """Puts a worker on hold, preventing scheduling of additional tasks to it.

        This is called "hold" mostly because this only stops scheduling of tasks,
        and does not actually kill the worker.

        Parameters
        ----------

        worker_id : str
            Worker id to be put on hold
        """
        c = self.command_client.run(f"HOLD_WORKER;{worker_id}")
        logger.debug(f"Sent hold request to worker: {worker_id}")
        return c

    def send_heartbeat(self):
        logger.warning("Sending heartbeat to interchange")
        msg = Heartbeat(endpoint_id="")
        self.outgoing_q.put(msg.pack())

    def wait_for_endpoint(self):
        heartbeat = self.command_client.run(HeartbeatReq())
        logger.debug("Attempting heartbeat to interchange")
        return heartbeat

    @property
    def outstanding(self):
        outstanding_c = self.command_client.run("OUTSTANDING_C")
        logger.debug(f"Got outstanding count: {outstanding_c}")
        return outstanding_c

    @property
    def connected_workers(self):
        workers = self.command_client.run("MANAGERS")
        logger.debug(f"Got managers: {workers}")
        return workers

    def submit(
        self, func, *args, container_id: str = "RAW", task_id: str = None, **kwargs
    ):
        """Submits the function and it's params for execution."""
        self._task_counter += 1
        if task_id is None:
            task_id = self._task_counter
        task_id = str(task_id)

        fn_code = fx_serializer.serialize(func)
        ser_code = fx_serializer.pack_buffers([fn_code])
        ser_params = fx_serializer.pack_buffers(
            [fx_serializer.serialize(args), fx_serializer.serialize(kwargs)]
        )
        payload = Task(task_id, container_id, ser_code + ser_params)

        self.submit_raw(payload.pack())
        self.tasks[task_id] = Future()

        return self.tasks[task_id]

    def submit_raw(self, packed_task):
        """Submits work to the the outgoing_q.

        The outgoing_q is an external process listens on this
        queue for new work. This method behaves like a
        submit call as described in the `Python docs \
        <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_

        Parameters
        ----------
        Packed Task (messages.Task) - A packed Task object which contains task_id,
        container_id, and serialized fn, args, kwargs packages.

        Returns:
              Submit status
        """
        logger.debug(f"Submitting raw task : {packed_task}")
        if self._executor_bad_state.is_set():
            raise self._executor_exception

        # Submit task to queue
        return self.outgoing_q.put(packed_task)

    def _get_block_and_job_ids(self):
        # Not using self.blocks.keys() and self.blocks.values() simultaneously
        # The dictionary may be changed during invoking this function
        # As scale_in and scale_out are invoked in multiple threads
        block_ids = list(self.blocks.keys())
        job_ids = []  # types: List[Any]
        for bid in block_ids:
            job_ids.append(self.blocks[bid])
        return block_ids, job_ids

    @property
    def connection_info(self):
        """All connection info necessary for the endpoint to connect back

        Returns:
              Dict with connection info
        """
        return {
            "address": self.address,
            # A memorial to the ungodly amount of time and effort spent,
            # troubleshooting the order of these ports.
            "client_ports": "{},{},{}".format(
                self.outgoing_q.port, self.incoming_q.port, self.command_client.port
            ),
        }

    @property
    def scaling_enabled(self):
        return self._scaling_enabled

    def scale_out(self, blocks=1):
        """Scales out the number of blocks by "blocks"

        Raises:
             NotImplementedError
        """
        r = []
        for i in range(blocks):
            if self.provider:
                block = self.provider.submit(self.launch_cmd, 1, 1)
                logger.debug(f"Launched block {i}:{block}")
                if not block:
                    raise (
                        ScalingFailed(
                            self.provider.label,
                            "Attempts to provision nodes via provider has failed",
                        )
                    )
                self.blocks.extend([block])
            else:
                logger.error("No execution provider available")
                r = None
        return r

    def scale_in(self, blocks):
        """Scale in the number of active blocks by specified amount.

        The scale in method here is very rude. It doesn't give the workers
        the opportunity to finish current tasks or cleanup. This is tracked
        in issue #530

        Raises:
             NotImplementedError
        """
        to_kill = self.blocks[:blocks]
        if self.provider:
            r = self.provider.cancel(to_kill)
        return r

    def _get_job_ids(self):
        return list(self.block.values())

    def status(self):
        """Return status of all blocks."""

        status = []
        if self.provider:
            status = self.provider.status(self.blocks)

        return status

    def shutdown(self, hub=True, targets="all", block=False):
        """Shutdown the executor, including all workers and controllers.

        This is not implemented.

        Kwargs:
            - hub (Bool): Whether the hub should be shutdown, Default:True,
            - targets (list of ints| 'all'): List of block id's to kill, Default:'all'
            - block (Bool): To block for confirmations or not

        Raises:
             NotImplementedError
        """

        logger.info("Attempting HighThroughputExecutor shutdown")
        # self.outgoing_q.close()
        # self.incoming_q.close()
        if self.queue_proc:
            self.queue_proc.terminate()
        logger.info("Finished HighThroughputExecutor shutdown attempt")
        return True

    @property
    def workers_per_node(self) -> Union[int, float]:
        return self.max_workers_per_node

    def _get_launch_command(self, block_id: str) -> str:
        if self.launch_cmd is None:
            raise ScalingFailed(self.provider.label, "No launch command")
        launch_cmd = self.launch_cmd.format(block_id=block_id)
        return launch_cmd


def executor_starter(htex, logdir, endpoint_id, logging_level=logging.DEBUG):
    stdout = open(os.path.join(logdir, f"executor.{endpoint_id}.stdout"), "w")
    stderr = open(os.path.join(logdir, f"executor.{endpoint_id}.stderr"), "w")

    logdir = os.path.abspath(logdir)
    with daemon.DaemonContext(stdout=stdout, stderr=stderr):
        global logger
        print("cwd: ", os.getcwd())
        logger = set_file_logger(
            os.path.join(logdir, f"executor.{endpoint_id}.log"),
            level=logging_level,
        )
        htex.start()

    stdout.close()
    stderr.close()
