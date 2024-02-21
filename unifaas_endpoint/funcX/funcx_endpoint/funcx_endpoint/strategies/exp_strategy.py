import logging
import math
import time

from parsl.providers.provider_base import JobState

from funcx_endpoint.strategies.base import BaseStrategy

logger = logging.getLogger("interchange.strategy.exp")


class ExpStrategy(BaseStrategy):
    """Implements the simple strategy"""

    def __init__(self, *args, threshold=20, interval=1, max_idletime=60, sleep_interval=30):
        """Initialize the flowcontrol object.

        We start the timer thread here

        Parameters
        ----------
        threshold:(int)
          Tasks after which the callback is triggered

        interval (int)
          seconds after which timer expires

        max_idletime: (int)
          maximum idle time(seconds) allowed for resources after which strategy will
          try to kill them.
          default: 60s

        """
        logger.info("ExpStrategy Initialized")
        super().__init__(*args, threshold=threshold, interval=interval)
        self.max_idletime = max_idletime
        self.sleep_interval = sleep_interval
        self.executors = {"idle_since": None}

    
    def update_idle_since(self):
        # the manager is not idle since there is a call
        self.executors["idle_since"] = None

    def strategize(self, *args, **kwargs):
        try:
            self._strategize(*args, **kwargs)
        except Exception as e:
            logger.exception(f"Caught error in strategize : {e}")
            pass

    def _strategize(self, *args, **kwargs):
        task_breakdown = self.interchange.get_outstanding_breakdown()
        logger.debug(f"Task breakdown {task_breakdown}")

        min_blocks = self.interchange.provider.min_blocks
        max_blocks = self.interchange.provider.max_blocks

        # Here we assume that each node has atleast 4 workers

        tasks_per_node = self.interchange.max_workers_per_node
        if self.interchange.max_workers_per_node == float("inf"):
            tasks_per_node = 1

        nodes_per_block = self.interchange.provider.nodes_per_block
        parallelism = self.interchange.provider.parallelism

        active_tasks = sum(self.interchange.get_total_tasks_outstanding().values())
        logger.debug(f"The number of current active_tasks {active_tasks}")
        status = self.interchange.provider_status()
        logger.debug(f"Provider status : {status}")

        running = sum([1 for x in status if x.state == JobState.RUNNING])
        pending = sum([1 for x in status if x.state == JobState.PENDING])
        active_blocks = running + pending
        active_slots = active_blocks * tasks_per_node * nodes_per_block

        logger.debug(
            "Endpoint has %s active tasks, %s/%s running/pending blocks, "
            "and %s connected workers",
            active_tasks,
            running,
            pending,
            self.interchange.get_total_live_workers(),
        )

        # reset kill timer if executor has active tasks
        if active_tasks > 0 and self.executors["idle_since"]:
            self.executors["idle_since"] = None

        # Case 1
        # No tasks.
        if active_tasks == 0:
            # Case 1a
            # Fewer blocks that min_blocks
            if active_blocks <= min_blocks:
                # Ignore
                # logger.debug("Strategy: Case.1a")
                pass

            # Case 1b
            # More blocks than min_blocks. Scale down
            else:
                # We want to make sure that max_idletime is reached
                # before killing off resources
                if not self.executors["idle_since"]:
                    logger.debug(
                        "Endpoint has 0 active tasks; starting kill timer "
                        "(if idle time exceeds %s seconds, resources will be removed)",
                        self.max_idletime,
                    )
                    self.executors["idle_since"] = time.time()

                idle_since = self.executors["idle_since"]
                if (time.time() - idle_since) > self.max_idletime:
                    # We have resources idle for the max duration,
                    # we have to scale_in now.
                    logger.info(
                        "Idle time has reached {}s; removing resources".format(
                            self.max_idletime
                        )
                    )

                else:
                    pass
                    # logger.debug(
                    #     "Strategy: Case.1b. Waiting for timer : %s", idle_since
                    # )

        # Case 2
        # More tasks than the available slots.
        elif (float(active_slots) / active_tasks) < parallelism:
            # Case 2a
            # We have the max blocks possible
            if active_blocks >= max_blocks:
                # Ignore since we already have the max nodes
                # logger.debug("Strategy: Case.2a")
                pass

            # Case 2b
            else:
                # logger.debug("Strategy: Case.2b")
                excess = math.ceil((active_tasks * parallelism) - active_slots)
                excess_blocks = math.ceil(
                    float(excess) / (tasks_per_node * nodes_per_block)
                )
                excess_blocks = min(excess_blocks, max_blocks - active_blocks)
                logger.debug(f"Requesting {excess_blocks} more blocks")

        elif active_slots == 0 and active_tasks > 0:
            # Case 4
            # Check if slots are being lost quickly ?
            logger.debug("Requesting single slot")
        # Case 3
        # tasks ~ slots
        else:
            # logger.debug("Strategy: Case 3")
            pass
