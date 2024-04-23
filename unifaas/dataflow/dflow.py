import atexit
import logging
import os
import pathlib
import pickle
import random
import time
import typeguard
import inspect
import threading
import sys
import datetime
from getpass import getuser
from typing import Any, Dict, List, Optional, Sequence
from uuid import uuid4
from socket import gethostname
from concurrent.futures import Future
from functools import partial
from unifaas.executors.funcx.executor import FuncXExecutor
from queue import PriorityQueue
import unifaas
from unifaas.app.errors import RemoteExceptionWrapper
from unifaas.config import Config
from unifaas.dataflow.error import (
    ConfigurationError,
    DependencyError,
    DuplicateTaskError,
)
from unifaas.dataflow.futures import AppFuture
from unifaas.dataflow.rundirs import make_rundir
from unifaas.dataflow.states import States, FINAL_STATES, FINAL_FAILURE_STATES
from unifaas.utils import get_version, get_std_fname_mode, get_all_checkpoints
from unifaas.dataflow.scheduler import Scheduler
from unifaas.dataflow.data_transfer_management import DataTransferManager
from unifaas.dataflow.helper.execution_recorder import ExecutionRecorder
from unifaas.dataflow.helper.resource_status_poller import ResourceStatusPoller
from unifaas.dataflow.helper.graph_helper import graphHelper
from unifaas.dataflow.helper.task_status_tracker import TaskStatusTracker
from unifaas.dataflow.task_launcher import TaskLauncher
from unifaas.compressor import compress_func
logger = logging.getLogger("unifaas")

exp_logger = logging.getLogger("experiment")


class DataFlowKernel(object):
    """The DataFlowKernel adds dependency awareness to an existing executor.

    It is responsible for managing futures, such that when dependencies are resolved,
    pending tasks move to the runnable state.

    Here is a simplified diagram of what happens internally::

         User             |        DFK         |    Executor
        ----------------------------------------------------------
                          |                    |
               Task-------+> +Submit           |
             App_Fu<------+--|                 |
                          |  Dependencies met  |
                          |         task-------+--> +Submit
                          |        Ex_Fu<------+----|

    """

    def __init__(self, config=Config()):
        """Initialize the DataFlowKernel.

        Parameters
        ----------
        config : Config
            A specification of all configuration options. For more details see the
            :class:~`unifaas.config.Config` documentation.
        """

        # this will be used to check cleanup only happens once
        self.cleanup_called = False
        self.enable_schedule = config.enable_schedule
        self.enable_execution_recorder = config.enable_execution_recorder
        if isinstance(config, dict):
            raise ConfigurationError(
                "Expected `Config` class, received dictionary. For help, "
                "see http://unifaas.readthedocs.io/en/stable/stubs/unifaas.config.Config.html"
            )
        self._config = config
        self.run_dir = make_rundir(config.run_dir)
        self.last_keytasks = {state: 0 for state in States}
        self.update_task_state_since = time.time()

        if config.initialize_logging:
            unifaas.set_file_logger(
                "{}/unifaas.log".format(self.run_dir),
                name="unifaas",
                level=logging.INFO,
            )
            unifaas.set_file_logger(
                "{}/exp.log".format(self.run_dir), name="experiment", level=logging.INFO
            )

        logger.debug("Starting DataFlowKernel with config\n{}".format(config))

        logger.info("Unifaas version: {}".format(get_version()))

        # Monitoring
        self.run_id = str(uuid4())
        self.tasks_completed_count = 0
        self.tasks_memo_completed_count = 0
        self.tasks_failed_count = 0
        self.tasks_dep_fail_count = 0

        # hub address and port for interchange to connect
        self.hub_address = None
        self.hub_interchange_port = None

        self.time_began = datetime.datetime.now()
        self.time_completed = None

        # TODO: make configurable
        logger.info("Run id is: " + self.run_id)

        self.workflow_name = None
        for frame in inspect.stack():
            fname = os.path.basename(str(frame.filename))
            parsl_file_names = ["dflow.py", "typeguard.py", "__init__.py"]
            # Find first file name not considered a parsl file
            if fname not in parsl_file_names:
                self.workflow_name = fname
                break
        else:
            self.workflow_name = "unnamed"
        self.workflow_name_at_predictor = config.workflow_name

        self.workflow_version = str(self.time_began.replace(microsecond=0))

        workflow_info = {
            "python_version": "{}.{}.{}".format(
                sys.version_info.major, sys.version_info.minor, sys.version_info.micro
            ),
            "parsl_version": get_version(),
            "time_began": self.time_began,
            "time_completed": None,
            "run_id": self.run_id,
            "workflow_name": self.workflow_name,
            "workflow_version": self.workflow_version,
            "rundir": self.run_dir,
            "tasks_completed_count": self.tasks_completed_count,
            "tasks_failed_count": self.tasks_failed_count,
            "user": getuser(),
            "host": gethostname(),
        }

        self.enable_duplicate = config.enable_duplicate

        self.executors = {}
        self.task_status_tracker = TaskStatusTracker()

        for executor in config.executors:
            if isinstance(executor, FuncXExecutor):
                executor.pre_data_trans = True
        self.add_executors(config.executors)
        self.status_poller = ResourceStatusPoller(config.executors)
        for executor_label in self.executors.keys():
            executor = self.executors[executor_label]
            if isinstance(executor, FuncXExecutor):
                executor.start_with_status_poller(self.status_poller)

        self.task_count = 0
        self.tasks = {}
        self.duplicated_tasks = {}
        self.submitter_lock = threading.Lock()

        atexit.register(self.atexit_cleanup)
        self._launch_priority_queue = PriorityQueue()
        self._kill_launch_in_period_event = threading.Event()
        self._task_launch_thread = threading.Thread(
            target=self.launch_in_period,
            args=(self._kill_launch_in_period_event,),
            name="Task-Launch-Thread",
        )
        self._task_launch_thread.daemon = True

        self._kill_report_in_period_event = threading.Event()
        self._report_interval = 60
        self._task_states_report_thread = threading.Thread(
            target=self.report_task_states_in_period,
            args=(self._kill_report_in_period_event, self._report_interval),
            name="Task-States-Report-Thread",
        )
        self._task_states_report_thread.daemon = True
        self.compress_task_tbl = {}

        self.data_trans_management = DataTransferManager(
            self.executors,
            password_file=config.password_file,
            bandwith_info=config.bandwidth_info,
            transfer_type=config.transfer_type,
        )

        # Temporarily, the recorder, status and predictor are initialized together
        if self.enable_execution_recorder:
            self.execution_recorder = ExecutionRecorder()
        else:
            self.execution_recorder = None

        if self.enable_schedule:
            self.task_launcher = TaskLauncher(
                self.executors,
                self.launch_if_ready,
                config.scheduling_strategy,
                self.task_status_tracker,
            )
            self._task_launch_thread.start()
            self._task_states_report_thread.start()
            self.scheduler = Scheduler(
                self.executors,
                enable_execution_predictor=True,
                scheduling_strategy=config.scheduling_strategy,
                recorder=self.execution_recorder,
                workflow_name=self.workflow_name_at_predictor,
                duplicated_tasks=self.duplicated_tasks,
                enable_duplicate=self.enable_duplicate,
            )

    def _send_task_log_info(self, task_record):
        pass

    def _create_task_log_info(self, task_record):
        """
        Create the dictionary that will be included in the log.
        """
        info_to_monitor = [
            "func_name",
            "memoize",
            "hashsum",
            "fail_count",
            "fail_cost",
            "status",
            "id",
            "time_invoked",
            "try_time_launched",
            "time_returned",
            "try_time_returned",
            "executor",
        ]

        task_log_info = {"task_" + k: task_record[k] for k in info_to_monitor}
        task_log_info["run_id"] = self.run_id
        task_log_info["try_id"] = task_record["try_id"]
        task_log_info["timestamp"] = datetime.datetime.now()
        task_log_info["task_status_name"] = task_record["status"].name
        task_log_info["tasks_failed_count"] = self.tasks_failed_count
        task_log_info["tasks_completed_count"] = self.tasks_completed_count
        task_log_info["tasks_memo_completed_count"] = self.tasks_memo_completed_count
        task_log_info["from_memo"] = task_record["from_memo"]
        task_log_info["task_inputs"] = str(task_record["kwargs"].get("inputs", None))
        task_log_info["task_outputs"] = str(task_record["kwargs"].get("outputs", None))
        task_log_info["task_stdin"] = task_record["kwargs"].get("stdin", None)
        stdout_spec = task_record["kwargs"].get("stdout", None)
        stderr_spec = task_record["kwargs"].get("stderr", None)
        try:
            stdout_name, _ = get_std_fname_mode("stdout", stdout_spec)
        except Exception as e:
            logger.warning(
                "Incorrect stdout format {} for Task {}".format(
                    stdout_spec, task_record["id"]
                )
            )
            stdout_name = str(e)
        try:
            stderr_name, _ = get_std_fname_mode("stderr", stderr_spec)
        except Exception as e:
            logger.warning(
                "Incorrect stderr format {} for Task {}".format(
                    stderr_spec, task_record["id"]
                )
            )
            stderr_name = str(e)
        task_log_info["task_stdout"] = stdout_name
        task_log_info["task_stderr"] = stderr_name
        task_log_info["task_fail_history"] = ",".join(task_record["fail_history"])
        task_log_info["task_depends"] = None
        if task_record["depends"] is not None:
            task_log_info["task_depends"] = ",".join(
                [str(t.tid) for t in task_record["depends"] if isinstance(t, AppFuture)]
            )

        j = task_record["joins"]
        if isinstance(j, AppFuture):
            task_log_info["task_joins"] = j.tid
        else:
            task_log_info["task_joins"] = None
        return task_log_info

    def _count_deps(self, depends):
        """Internal.

        Count the number of unresolved futures in the list depends.
        """
        count = 0
        for dep in depends:
            if isinstance(dep, Future):
                if not dep.done():
                    count += 1

        return count

    @property
    def config(self):
        """Returns the fully initialized config that the DFK is actively using.

        Returns:
             - config (dict)
        """
        return self._config

    def handle_exec_update(self, task_record, future, duplicated=False):
        """This function is called only as a callback from an execution
        attempt reaching a final state (either successfully or failing).

        It will launch retries if necessary, and update the task
        structure.

        Args:
             task_record (dict) : Task record
             future (Future) : The future object corresponding to the task which
             makes this callback
        """
        if task_record["status"] == States.exec_done:
            executor = task_record["executor"]
            if self.status_poller.real_time_status[executor]["pending_tasks"] > 0:
                self.status_poller.real_time_status[executor]["pending_tasks"] -= 1
            else:
                self.status_poller.real_time_status[executor]["idle_workers"] += 1
            return
        exp_logger.debug(f"[latency] Task done")
        task_id = task_record["id"]
        if "original_task" in task_record.keys():
            # It is an duplicated task
            # Check whether the original task is done
            original_task = task_record["original_task"]
            if original_task["status"] == States.exec_done:
                if task_record["status"] != States.exec_done:
                    executor = task_record["executor"]
                    if (
                        self.status_poller.real_time_status[executor]["pending_tasks"]
                        > 0
                    ):
                        self.status_poller.real_time_status[executor][
                            "pending_tasks"
                        ] -= 1
                    else:
                        self.status_poller.real_time_status[executor][
                            "idle_workers"
                        ] += 1
                    self._complete_task(task_record, States.exec_done, future)
                return
            else:
                # use this result to update the original task
                exp_logger.info(
                    f"[Duplicate] Original task {original_task['id']} is replaced by task {task_id}"
                )
                self.handle_exec_update(original_task, future, duplicated=True)
                self._complete_task(task_record, States.exec_done, future)
                return

        task_record["try_time_returned"] = datetime.datetime.now()

        if not future.done():
            raise ValueError(
                "done callback called, despite future not reporting itself as done"
            )

        try:
            res = self._unwrap_remote_exception_wrapper(future)

        except Exception as e:
            logger.debug("Task {} try {} failed".format(task_id, task_record["try_id"]))
            # We keep the history separately, since the future itself could be
            # tossed.
            task_record["fail_history"].append(repr(e))
            task_record["fail_count"] += 1
            if self._config.retry_handler:
                try:
                    cost = self._config.retry_handler(e, task_record)
                except Exception as retry_handler_exception:
                    logger.exception(
                        "retry_handler raised an exception - will not retry"
                    )

                    # this can be any amount > self._config.retries, to stop any more
                    # retries from happening
                    task_record["fail_cost"] = self._config.retries + 1

                    # make the reported exception be the retry handler's exception,
                    # rather than the execution level exception
                    e = retry_handler_exception
                else:
                    task_record["fail_cost"] += cost
            else:
                task_record["fail_cost"] += 1

            if task_record["status"] == States.dep_fail:
                logger.info(
                    "Task {} failed due to dependency failure so skipping retries".format(
                        task_id
                    )
                )
                task_record["time_returned"] = datetime.datetime.now()
                with task_record["app_fu"]._update_lock:
                    task_record["app_fu"].set_exception(e)

            elif task_record["fail_cost"] <= self._config.retries:
                # record the final state for this try before we mutate for retries
                task_record["status"] = States.fail_retryable
                self._send_task_log_info(task_record)

                task_record["try_id"] += 1
                task_record["status"] = States.pending
                task_record["try_time_launched"] = None
                task_record["try_time_returned"] = None
                task_record["fail_history"] = []

                logger.info("Task {} marked for retry".format(task_id))

            else:
                logger.exception(
                    "Task {} failed after {} retry attempts".format(
                        task_id, task_record["try_id"]
                    )
                )
                task_record["time_returned"] = datetime.datetime.now()
                task_record["status"] = States.failed
                self.tasks_failed_count += 1
                task_record["time_returned"] = datetime.datetime.now()
                with task_record["app_fu"]._update_lock:
                    task_record["app_fu"].set_exception(e)
            self.task_status_tracker.update_when_task_done(task_record)
            if self.enable_schedule:
                self.scheduler.dynamic_adjust(task_record, task_res=None)

        else:
            if isinstance(res, dict):
                logger.debug("Task {} completed with result: {}".format(task_id, res))
                exp_logger.debug(
                    f"[MicroExp] Task {task_record['id']} completed at {time.time()}"
                )
                if self.enable_execution_recorder:
                    predict_time = 0
                    if "predict_execution" in task_record.keys():
                        executor = (
                            task_record["executor"]
                            if duplicated == False
                            else task_record["duplicated_at"]
                        )
                        predict_time = task_record["predict_execution"][executor]
                    res["predict_time"] = predict_time
                    self.execution_recorder.write_record(task_id, res)
                task_record["output_size"] = res["output_size"]
                exp_logger.info(
                    f"[CaseStudy] Task {task_record['id']} completed: {res['execution_time']}|{res['cpu_percent']}|{res['output_size']}"
                )
                if duplicated == False:
                    self.status_poller.update_from_result_handler(
                        res, task_record["executor"], task_record
                    )
                else:
                    self.status_poller.update_from_result_handler(
                        res, task_record["duplicated_at"], task_record
                    )
                res = res["result"]
                self.task_status_tracker.update_when_task_done(task_record)
            if task_record["from_memo"]:
                self._complete_task(task_record, States.memo_done, res)
            else:
                if not task_record["join"]:
                    self._complete_task(task_record, States.exec_done, res)
                else:
                    # This is a join task, and the original task's function code has
                    # completed. That means that the future returned by that code
                    # will be available inside the executor future, so we can now
                    # record the inner app ID in monitoring, and add a completion
                    # listener to that inner future.

                    inner_future = future.result()

                    # Fail with a TypeError if the joinapp python body returned
                    # something we can't join on.
                    if isinstance(inner_future, Future):
                        task_record["status"] = States.joining
                        task_record["joins"] = inner_future
                        inner_future.add_done_callback(
                            partial(self.handle_join_update, task_record)
                        )
                    else:
                        task_record["time_returned"] = datetime.datetime.now()
                        task_record["status"] = States.failed
                        self.tasks_failed_count += 1
                        task_record["time_returned"] = datetime.datetime.now()
                        with task_record["app_fu"]._update_lock:
                            task_record["app_fu"].set_exception(
                                TypeError(
                                    f"join_app body must return a Future, got {type(inner_future)}"
                                )
                            )
            if self.enable_schedule:
                self.scheduler.dynamic_adjust(task_record, task_res=res)

        self._log_std_streams(task_record)
        # record current state for this task: maybe a new try, maybe the original try marked as failed, maybe the original try joining
        self._send_task_log_info(task_record)

        # it might be that in the course of the update, we've gone back to being
        # pending - in which case, we should consider ourself for relaunch
        if task_record["status"] == States.pending:
            self.launch_if_ready(task_record)

    def handle_join_update(self, task_record, inner_app_future):
        # Use the result of the inner_app_future as the final result of
        # the outer app future.

        # There is no retry handling here: inner apps are responsible for
        # their own retrying, and joining state is responsible for passing
        # on whatever the result of that retrying was (if any).

        outer_task_id = task_record["id"]

        if inner_app_future.exception():
            e = inner_app_future.exception()
            logger.debug(
                "Task {} failed due to failure of inner join future".format(
                    outer_task_id
                )
            )
            # We keep the history separately, since the future itself could be
            # tossed.
            task_record["fail_history"].append(repr(e))
            task_record["fail_count"] += 1
            # no need to update the fail cost because join apps are never
            # retried

            task_record["status"] = States.failed
            self.tasks_failed_count += 1
            task_record["time_returned"] = datetime.datetime.now()
            with task_record["app_fu"]._update_lock:
                task_record["app_fu"].set_exception(e)

        else:
            res = inner_app_future.result()
            self._complete_task(task_record, States.exec_done, res)

        self._log_std_streams(task_record)

        self._send_task_log_info(task_record)

    def handle_app_update(self, task_record, future):
        """This function is called as a callback when an AppFuture
        is in its final state.

        It will trigger post-app processing such as checkpointing.

        Args:
             task_record : Task record
             future (Future) : The relevant app future (which should be
                 consistent with the task structure 'app_fu' entry

        """

        task_id = task_record["id"]

        if not task_record["app_fu"].done():
            logger.error(
                "Internal consistency error: app_fu is not done for task {}".format(
                    task_id
                )
            )
        if not task_record["app_fu"] == future:
            logger.error(
                "Internal consistency error: callback future is not the app_fu in task structure, for task {}".format(
                    task_id
                )
            )
        return

    def _complete_task(self, task_record, new_state, result):
        """Set a task into a completed state"""
        assert new_state in FINAL_STATES
        assert new_state not in FINAL_FAILURE_STATES
        old_state = task_record["status"]
        task_record["status"] = new_state

        if new_state == States.exec_done:
            self.tasks_completed_count += 1
        elif new_state == States.memo_done:
            self.tasks_memo_completed_count += 1
        else:
            raise RuntimeError(
                f"Cannot update task counters with unknown final state {new_state}"
            )

        logger.info(
            f"Task {task_record['id']} completed ({old_state.name} -> {new_state.name})"
        )
        task_record["time_returned"] = datetime.datetime.now()

        with task_record["app_fu"]._update_lock:
            task_record["app_fu"].set_result(result)

    """ When the result is wrapped with a dictionary. 
        The real result is stored at dict['result']
    """

    @staticmethod
    def _unwrap_remote_exception_wrapper(future: Future) -> Any:
        result = future.result()
        if isinstance(result, dict) and "result" in result.keys():
            if isinstance(result["result"], RemoteExceptionWrapper):
                result["result"].reraise()
        else:
            if isinstance(result, RemoteExceptionWrapper):
                result.reraise()
        return result

    def wipe_task(self, task_id):
        """Remove task with task_id from the internal tasks table"""
        if self.config.garbage_collect:
            if task_id in self.tasks.keys():
                del self.tasks[task_id]

    @staticmethod
    def check_staging_inhibited(kwargs):
        return kwargs.get("_parsl_staging_inhibit", False)

    def launch_if_ready(self, task_record):
        """
        launch_if_ready will launch the specified task, if it is ready
        to run (for example, without dependencies, and in pending state).

        This should be called by any piece of the DataFlowKernel that
        thinks a task may have become ready to run.

        It is not an error to call launch_if_ready on a task that is not
        ready to run - launch_if_ready will not incorrectly launch that
        task.

        launch_if_ready is thread safe, so may be called from any thread
        or callback.
        """
        if (
            not task_record["status"] == States.pending
            and not task_record["status"] == States.queuing
        ):
            return

        task_id = task_record["id"]
        if self._count_deps(task_record["depends"]) == 0:
            # We can now launch *task*
            new_args, kwargs, exceptions_tids = self.sanitize_and_wrap(
                task_record["args"], task_record["kwargs"]
            )
            task_record["args"] = new_args
            task_record["kwargs"] = kwargs
            if not exceptions_tids:
                # There are no dependency errors
                exec_fu = None
                # Acquire a lock, retest the state, launch
                with task_record["task_launch_lock"]:
                    if (
                        task_record["status"] == States.pending
                        or task_record["status"] == States.queuing
                    ):
                        try:
                            if not task_record["submitted_to_poller"]:
                                self.status_poller.update_status_when_submit_one_task(
                                    task_record["executor"]
                                )
                                task_record["submitted_to_poller"] = True
                            exp_logger.debug(
                                f"[MicroExp] Task {task_record['id']} launch_task on executor at time {time.time()}"
                            )
                            self.scheduler.put_important_task_into_duplicated_queue(
                                task_record
                            )
                            self.task_status_tracker.update_when_task_submit_to_executor(
                                task_record
                            )
                            exec_fu = self.launch_task(
                                task_record, task_record["func"], *new_args, **kwargs
                            )
                            assert isinstance(exec_fu, Future)
                        except Exception as e:
                            # task launched failed somehow. the execution might
                            # have been launched and an exception raised after
                            # that, though. that's hard to detect from here.
                            # we don't attempt retries here. This is an error with submission
                            # even though it might come from user code such as a plugged-in
                            # executor or memoization hash function.

                            logger.debug(
                                "Got an exception launching task", exc_info=True
                            )
                            exec_fu = Future()
                            exec_fu.set_exception(e)
            else:
                logger.info("Task {} failed due to dependency failure".format(task_id))
                # Raise a dependency exception
                task_record["status"] = States.dep_fail
                self.tasks_dep_fail_count += 1

                self._send_task_log_info(task_record)

                exec_fu = Future()
                exec_fu.set_exception(DependencyError(exceptions_tids, task_id))

            if exec_fu:
                assert isinstance(exec_fu, Future)
                try:
                    exec_fu.add_done_callback(
                        partial(self.handle_exec_update, task_record)
                    )
                except Exception:
                    # this exception is ignored here because it is assumed that exception
                    # comes from directly executing handle_exec_update (because exec_fu is
                    # done already). If the callback executes later, then any exception
                    # coming out of the callback will be ignored and not propate anywhere,
                    # so this block attempts to keep the same behaviour here.
                    logger.error(
                        "add_done_callback got an exception which will be ignored",
                        exc_info=True,
                    )

                task_record["exec_fu"] = exec_fu

    def launch_task(self, task_record, executable, *args, **kwargs):
        """Handle the actual submission of the task to the executor layer.

        If the app task has the executors attributes not set (default=='all')
        the task is launched on a randomly selected executor from the
        list of executors. This behavior could later be updated to support
        binding to executors based on user specified criteria.

        If the app task specifies a particular set of executors, it will be
        targeted at those specific executors.

        Args:
            task_record : The task record
            executable (callable) : A callable object
            args (list of positional args)
            kwargs (arbitrary keyword arguments)


        Returns:
            Future that tracks the execution of the submitted executable
        """
        task_id = task_record["id"]
        task_record["try_time_launched"] = datetime.datetime.now()

        task_record["from_memo"] = False
        executor_label = task_record["executor"]
        try:
            executor = self.executors[executor_label]
        except Exception:
            logger.exception(
                "Task {} requested invalid executor {}: config is\n{}".format(
                    task_id, executor_label, self._config
                )
            )
            raise ValueError(
                "Task {} requested invalid executor {}".format(task_id, executor_label)
            )

        with self.submitter_lock:
            exp_logger.info(
                f"[CaseStudy] Task {task_record['id']} launch: {task_record['executor']}|{task_record['func_name']}"
            )
            exec_fu = executor.submit(
                executable, task_record["resource_specification"], *args, **kwargs
            )
        task_record["status"] = States.launched

        self._send_task_log_info(task_record)

        logger.debug("Task {} launched on executor {}".format(task_id, executor.label))

        self._log_std_streams(task_record)

        return exec_fu

    def _gather_all_deps(
        self, args: Sequence[Any], kwargs: Dict[str, Any]
    ) -> List[Future]:
        """Assemble a list of all Futures passed as arguments, kwargs or in the inputs kwarg.

        Args:
            - args: The list of args pass to the app
            - kwargs: The dict of all kwargs passed to the app

        Returns:
            - list of dependencies

        """
        depends: List[Future] = []

        def check_dep(d):
            if isinstance(d, Future):
                depends.extend([d])
            if isinstance(d, list):
                for app in d:
                    if isinstance(app, Future):
                        depends.extend([app])

        # Check the positional args
        for dep in args:
            check_dep(dep)

        # Check for explicit kwargs ex, fu_1=<fut>
        for key in kwargs:
            dep = kwargs[key]
            check_dep(dep)

        # Check for futures in inputs=[<fut>...]
        for dep in kwargs.get("inputs", []):
            check_dep(dep)
        
        # check the future should be compressed
        res_depends = []
        for dep in depends:
            if dep in self.compress_task_tbl:
                res_depends.append(self.compress_task_tbl[dep])
            else:
                res_depends.append(dep)


        return res_depends

    def sanitize_and_wrap(self, args, kwargs):
        """This function should be called when all dependencies have completed.

        It will rewrite the arguments for that task, replacing each Future
        with the result of that future.

        If the user hid futures a level below, we will not catch
        it, and will (most likely) result in a type error.

        Args:
             args (List) : Positional args to app function
             kwargs (Dict) : Kwargs to app function

        Return:
            a rewritten args list
            a rewritten kwargs dict
            pairs of exceptions, task ids from any Futures which stored
            exceptions rather than results.
        """
        dep_failures = []

        # Replace item in args
        new_args = []
        for dep in args:
            if isinstance(dep, Future):
                try:
                    new_args.extend([dep.result()])
                except Exception as e:
                    if hasattr(dep, "task_def"):
                        tid = dep.task_def["id"]
                    else:
                        tid = None
                    dep_failures.extend([(e, tid)])
            elif isinstance(dep, list):
                flag = False
                for app in dep:
                    if isinstance(app, Future):
                        try:
                            app.result()
                        except Exception as e:
                            if hasattr(app, "task_def"):
                                tid = app.task_def["id"]
                            else:
                                tid = None
                            dep_failures.extend([(e, tid)])
                            flag = True
                            break
                if not flag:
                    new_args.extend([dep])
            else:
                new_args.extend([dep])

        # Check for explicit kwargs ex, fu_1=<fut>
        for key in kwargs:
            dep = kwargs[key]
            if isinstance(dep, Future):
                try:
                    kwargs[key] = dep.result()
                except Exception as e:
                    if hasattr(dep, "task_def"):
                        tid = dep.task_def["id"]
                    else:
                        tid = None
                    dep_failures.extend([(e, tid)])

        # Check for futures in inputs=[<fut>...]
        if "inputs" in kwargs:
            new_inputs = []
            for dep in kwargs["inputs"]:
                if isinstance(dep, Future):
                    try:
                        new_inputs.extend([dep.result()])
                    except Exception as e:
                        if hasattr(dep, "task_def"):
                            tid = dep.task_def["id"]
                        else:
                            tid = None
                        dep_failures.extend([(e, tid)])

                else:
                    new_inputs.extend([dep])
            kwargs["inputs"] = new_inputs

        return new_args, kwargs, dep_failures

    def submit(
        self,
        func,
        app_args,
        executors="all",
        cache=False,
        ignore_for_cache=None,
        app_kwargs={},
        join=False,
        never_change=False,
        compressor=None,
    ):
        """Add task to the dataflow system.

        If the app task has the executors attributes not set (default=='all')
        the task will be launched on a randomly selected executor from the
        list of executors. If the app task specifies a particular set of
        executors, it will be targeted at the specified executors.

        Args:
            - func : A function object

        KWargs :
            - app_args : Args to the function
            - executors (list or string) : List of executors this call could go to.
                    Default='all'
            - cache (Bool) : To enable memoization or not
            - ignore_for_cache (list) : List of kwargs to be ignored for memoization/checkpointing
            - app_kwargs (dict) : Rest of the kwargs to the fn passed as dict.
            - compressor: claim what compresor will be used to compress the output data

        Returns:
               (AppFuture) [DataFutures,]

        """
        exp_logger.debug(f"[latency test] submit task to DFK")
        if ignore_for_cache is None:
            ignore_for_cache = []

        if self.cleanup_called:
            raise ValueError("Cannot submit to a DFK that has been cleaned up")

        task_id = self.task_count
        self.task_count += 1
        if isinstance(executors, str) and executors.lower() == "all":
            choices = list(e for e in self.executors if e != "_parsl_internal")
        elif isinstance(executors, list):
            choices = executors
        else:
            raise ValueError(
                "Task {} supplied invalid type for executors: {}".format(
                    task_id, type(executors)
                )
            )
        executor = random.choice(choices)
        # The below uses func.__name__ before it has been wrapped by any staging code.

        label = app_kwargs.get("label")
        for kw in ["stdout", "stderr"]:
            if kw in app_kwargs:
                if app_kwargs[kw] == unifaas.AUTO_LOGNAME:
                    if kw not in ignore_for_cache:
                        ignore_for_cache += [kw]
                    app_kwargs[kw] = os.path.join(
                        self.run_dir,
                        "task_logs",
                        str(int(task_id / 10000)).zfill(
                            4
                        ),  # limit logs to 10k entries per directory
                        "task_{}_{}{}.{}".format(
                            str(task_id).zfill(4),
                            func.__name__,
                            "" if label is None else "_{}".format(label),
                            kw,
                        ),
                    )

        resource_specification = app_kwargs.get("unifaas_resource_specification", {})

        task_def = {
            "depends": None,
            "executor": executor,
            "func_name": func.__name__,
            "memoize": cache,
            "hashsum": None,
            "exec_fu": None,
            "fail_count": 0,
            "fail_cost": 0,
            "fail_history": [],
            "from_memo": None,
            "ignore_for_cache": ignore_for_cache,
            "join": join,
            "joins": None,
            "status": States.unsched,
            "try_id": 0,
            "id": task_id,
            "time_invoked": datetime.datetime.now(),
            "time_returned": None,
            "try_time_launched": None,
            "try_time_returned": None,
            "resource_specification": resource_specification,
            "modified_times": 0,
            "submitted_to_poller": False,
            "never_change": never_change,
            "important": False,
            "compressor": compressor,
        }
        exp_logger.debug("submitting task {} to executor {}".format(task_id, executor))
        exp_logger.debug(
            f"[MicroExp] Task {task_id} submitted on executor at time {time.time()}"
        )
        app_fu = AppFuture(task_def)

        task_def.update(
            {"args": app_args, "func": func, "kwargs": app_kwargs, "app_fu": app_fu}
        )

        if task_id in self.tasks:
            raise DuplicateTaskError(
                "internal consistency error: Task {0} already exists in task list".format(
                    task_id
                )
            )
        else:
            self.tasks[task_id] = task_def

        # Get the list of dependencies for the task
        depends = self._gather_all_deps(app_args, app_kwargs)
        task_def["depends"] = depends

        depend_descs = []
        for d in depends:
            if isinstance(d, AppFuture):
                depend_descs.append("task {}".format(d.tid))
            else:
                depend_descs.append(repr(d))

        if depend_descs != []:
            waiting_message = "waiting on {}".format(", ".join(depend_descs))
        else:
            waiting_message = "not waiting on any dependency"

        logger.debug(
            "Task {} submitted for App {}, {}".format(
                task_id, task_def["func_name"], waiting_message
            )
        )

        task_def["task_launch_lock"] = threading.Lock()
        task_def["task_data_trans_lock"] = threading.Lock()
        task_def["data_trans_times"] = 0
        task_def["handle_managing_times"] = 0

        app_fu.add_done_callback(partial(self.handle_app_update, task_def))

        logger.debug(
            "Task {} set to pending state with AppFuture: {}".format(
                task_id, task_def["app_fu"]
            )
        )

        self.task_status_tracker.update_when_submit_to_dfk(task_def)

        self._send_task_log_info(task_def)

        # at this point add callbacks to all dependencies to do a launch_if_ready
        # call whenever a dependency completes.

        # we need to be careful about the order of setting the state to pending,
        # adding the callbacks, and caling launch_if_ready explicitly once always below.

        # I think as long as we call launch_if_ready once after setting pending, then
        # we can add the callback dependencies at any point: if the callbacks all fire
        # before then, they won't cause a launch, but the one below will. if they fire
        # after we set it pending, then the last one will cause a launch, and the
        # explicit one won't.

        if not self.enable_schedule:
            for d in depends:

                def callback_adapter(dep_fut):
                    self.launch_if_ready(task_def)

                try:
                    d.add_done_callback(callback_adapter)
                except Exception as e:
                    logger.error(
                        "add_done_callback got an exception {} which will be ignored".format(
                            e
                        )
                    )

        if self.enable_schedule:
            task_def["status"] = States.scheduling
            graphHelper.put_scheduling_task(task_def)
            self.scheduler.put_scheduling_task(task_def)
            exp_logger.debug(f"[latency test] put task to sheduling queue")
            self.data_trans_management.put_data_management_task(task_def)
            # if int(task_def['id']) % 1000 == 1:
            logger.debug(
                "Task {} submitted for App {}, {}".format(
                    task_id, task_def["func_name"], waiting_message
                )
            )

        else:
            task_def["status"] = States.pending
        self.launch_if_ready(task_def)

        if compressor is not None:
            self.append_compress_task(task_def, app_fu)

        return app_fu

    # it might also be interesting to assert that all DFK
    # tasks are in a "final" state (3,4,5) when the DFK
    # is closed down, and report some kind of warning.
    # although really I'd like this to drain properly...
    # and a drain function might look like this.
    # If tasks have their states changed, this won't work properly
    # but we can validate that...
    def log_task_states(self):
        logger.info("Summary of tasks in DFK:")

        keytasks = {state: 0 for state in States}

        for tid in self.tasks:
            keytasks[self.tasks[tid]["status"]] += 1
        # Fetch from counters since tasks get wiped
        keytasks[States.exec_done] = self.tasks_completed_count
        keytasks[States.memo_done] = self.tasks_memo_completed_count
        keytasks[States.failed] = self.tasks_failed_count
        keytasks[States.dep_fail] = self.tasks_dep_fail_count

        for state in States:
            if keytasks[state] != self.last_keytasks[state]:
                self.update_task_state_since = time.time()
                self.last_keytasks[state] = keytasks[state]

            if keytasks[state]:
                logger.info("Tasks in state {}: {}".format(str(state), keytasks[state]))

        # total_summarized = sum(keytasks.values())
        # tot_count = self.task_count + (self.scheduler.duplicated_task_count - self.scheduler.base_count_for_duplicated_task)
        # tot_count = self.task_count
        # if total_summarized != tot_count:
        #     logger.warning(
        #         "Task count summarisation was inconsistent: summarised {} tasks, but task counter registered {} tasks".format(
        #             total_summarized, tot_count))
        logger.info("End of summary")
        import os

        if time.time() - self.update_task_state_since > 60 * 60 * 3:
            logger.info("No task state update for 3 hours minutes, exit")
            os._exit(1)

    def _create_remote_dirs_over_channel(self, provider, channel):
        """Create script directories across a channel

        Parameters
        ----------
        provider: Provider obj
           Provider for which scritps dirs are being created
        channel: Channel obk
           Channel over which the remote dirs are to be created
        """
        run_dir = self.run_dir
        if channel.script_dir is None:
            channel.script_dir = os.path.join(run_dir, "submit_scripts")

            # Only create dirs if we aren't on a shared-fs
            if not channel.isdir(run_dir):
                parent, child = pathlib.Path(run_dir).parts[-2:]
                remote_run_dir = os.path.join(parent, child)
                channel.script_dir = os.path.join(
                    remote_run_dir, "remote_submit_scripts"
                )
                provider.script_dir = os.path.join(run_dir, "local_submit_scripts")

        channel.makedirs(channel.script_dir, exist_ok=True)

    def append_compress_task(self, to_be_compressed_task, cur_appfu):
        # firstly create a task record, then add this relation to table
        app = self.submit(func=compress_func, app_args=tuple([cur_appfu]))
        self.compress_task_tbl[to_be_compressed_task['app_fu']] = app 


    def add_executors(self, executors):
        for executor in executors:
            executor.run_id = self.run_id
            executor.run_dir = self.run_dir
            executor.hub_address = self.hub_address
            executor.hub_port = self.hub_interchange_port
            if hasattr(executor, "provider"):
                if hasattr(executor.provider, "script_dir"):
                    executor.provider.script_dir = os.path.join(
                        self.run_dir, "submit_scripts"
                    )
                    os.makedirs(executor.provider.script_dir, exist_ok=True)

                    if hasattr(executor.provider, "channels"):
                        logger.debug("Creating script_dir across multiple channels")
                        for channel in executor.provider.channels:
                            self._create_remote_dirs_over_channel(
                                executor.provider, channel
                            )
                    else:
                        self._create_remote_dirs_over_channel(
                            executor.provider, executor.provider.channel
                        )

            self.executors[executor.label] = executor
            block_ids = executor.start()

    def atexit_cleanup(self):
        if not self.cleanup_called:
            self.cleanup()

    def wait_for_current_tasks(self):
        """Waits for all tasks in the task list to be completed, by waiting for their
        AppFuture to be completed. This method will not necessarily wait for any tasks
        added after cleanup has started (such as data stageout?)
        """

        logger.info("Waiting for all remaining tasks to complete")
        for task_id in list(self.tasks):
            # .exception() is a less exception throwing way of
            # waiting for completion than .result()
            if task_id not in self.tasks:
                logger.debug("Task {} no longer in task list".format(task_id))
            else:
                task_record = self.tasks[
                    task_id
                ]  # still a race condition with the above self.tasks if-statement
                fut = task_record["app_fu"]
                if not fut.done():
                    fut.exception()
                # now app future is done, poll until DFK state is final: a DFK state being final and the app future being done do not imply each other.
                while task_record["status"] not in FINAL_STATES:
                    time.sleep(0.1)

        logger.info("All remaining tasks completed")

    def cleanup(self):
        """DataFlowKernel cleanup.

        This involves releasing all resources explicitly.

        If the executors are managed by the DFK, then we call scale_in on each of
        the executors and call executor.shutdown. Otherwise, executor cleanup is left to
        the user.
        """
        logger.info("DFK cleanup initiated")

        # this check won't detect two DFK cleanups happening from
        # different threads extremely close in time because of
        # non-atomic read/modify of self.cleanup_called
        if self.cleanup_called:
            raise Exception(
                "attempt to clean up DFK when it has already been cleaned-up"
            )
        self.cleanup_called = True

        self.log_task_states()

        for executor in self.executors.values():
            if executor.managed and not executor.bad_state_is_set:
                if executor.scaling_enabled:
                    job_ids = executor.provider.resources.keys()
                    block_ids = executor.scale_in(len(job_ids))

                executor.shutdown()

        self.time_completed = datetime.datetime.now()

        if self.enable_schedule:
            self.kill_launch_in_period_thread()
            self.scheduler.kill_scheduler()
            if self.execution_recorder:
                self.execution_recorder.kill_writer()

        logger.info("DFK cleanup complete")

    def checkpoint(self, tasks=None):
        # Not supported in UniFaaS. It is a feature implemented in Parsl
        pass

    def _load_checkpoints(self, checkpointDirs):
        # Not supported in UniFaaS. It is a feature implemented in Parsl
        pass

    def load_checkpoints(self, checkpointDirs):
        # Not supported in UniFaaS. It is a feature implemented in Parsl
        pass

    # TODO optimize the launch procedure
    # for example: set a counter to record the launch sequence
    def launch_in_period(self, kill_event):
        while not kill_event.is_set():
            time.sleep(1)
            start_time = time.time()
            for task_id in list(self.tasks):
                if task_id not in self.tasks:
                    logger.debug("Task {} no longer in task list".format(task_id))
                else:
                    task_record = self.tasks[task_id]
                    if task_record["status"] == States.pending:
                        # self.launch_if_ready(task_record)
                        self._launch_priority_queue.put(TaskWithPriority(task_record))

            for task_id in list(self.duplicated_tasks):
                if task_id not in self.duplicated_tasks:
                    logger.debug(
                        "Task {} no longer in duplicated task list".format(task_id)
                    )
                else:
                    task_record = self.duplicated_tasks[task_id]
                    if task_record["status"] == States.pending:
                        # self.launch_if_ready(task_record)
                        self._launch_priority_queue.put(TaskWithPriority(task_record))

            while self._launch_priority_queue.qsize() > 0:
                task_record = self._launch_priority_queue.get().task_record
                if task_record["status"] == States.pending:
                    self.task_launcher.dispatch_task_record_to_launch_que(task_record)
                    # self.launch_if_ready(task_record)
            self.task_launcher.try_launch()
            exp_logger.debug(f"launch_in_period cost: {time.time() - start_time}")

    def report_task_states_in_period(self, kill_event, interval=60):
        while not kill_event.is_set():
            self.log_task_states()
            time.sleep(interval)

    def kill_launch_in_period_thread(self):
        self._kill_launch_in_period_event.set()
        self._task_launch_thread.join()

    @staticmethod
    def _log_std_streams(task_record):
        if task_record["app_fu"].stdout is not None:
            logger.info(
                "Standard output for task {} available at {}".format(
                    task_record["id"], task_record["app_fu"].stdout
                )
            )
        if task_record["app_fu"].stderr is not None:
            logger.info(
                "Standard error for task {} available at {}".format(
                    task_record["id"], task_record["app_fu"].stderr
                )
            )


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


class DataFlowKernelLoader(object):
    """Manage which DataFlowKernel is active.

    This is a singleton class containing only class methods. You should not
    need to instantiate this class.
    """

    _dfk = None

    @classmethod
    def clear(cls):
        """Clear the active DataFlowKernel so that a new one can be loaded."""
        cls._dfk = None

    @classmethod
    @typeguard.typechecked
    def load(cls, config: Optional[Config] = None):
        """Load a DataFlowKernel.

        Args:
            - config (Config) : Configuration to load. This config will be passed to a
              new DataFlowKernel instantiation which will be set as the active DataFlowKernel.
        Returns:
            - DataFlowKernel : The loaded DataFlowKernel object.
        """
        if cls._dfk is not None:
            raise RuntimeError("Config has already been loaded")

        if config is None:
            cls._dfk = DataFlowKernel(Config())
        else:
            cls._dfk = DataFlowKernel(config)

        return cls._dfk

    @classmethod
    def wait_for_current_tasks(cls):
        """Waits for all tasks in the task list to be completed, by waiting for their
        AppFuture to be completed. This method will not necessarily wait for any tasks
        added after cleanup has started such as data stageout.
        """
        cls.dfk().wait_for_current_tasks()

    @classmethod
    def dfk(cls):
        """Return the currently-loaded DataFlowKernel."""
        if cls._dfk is None:
            raise RuntimeError("Must first load config")
        return cls._dfk
