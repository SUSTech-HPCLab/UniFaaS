import math
import random


def naive_scheduler(
    task_qs, outstanding_task_count, max_workers, old_worker_map, to_die_list, logger
):
    """
    Return two items (as one tuple)
        dict kill_list :: KILL [(worker_type, num_kill), ...]
        dict create_list :: CREATE [(worker_type, num_create), ...]

    In this scheduler model, there is minimum 1 instance of each nonempty task queue.
    """

    logger.debug("Entering scheduler...")
    logger.debug(f"old_worker_map: {old_worker_map}")
    q_sizes = {}
    q_types = []
    new_worker_map = {}

    # Sum the size of each *available* (unblocked) task queue
    sum_q_size = 0
    for q_type in outstanding_task_count:
        q_types.append(q_type)
        q_size = outstanding_task_count[q_type]
        sum_q_size += q_size
        q_sizes[q_type] = q_size

    if sum_q_size > 0:
        logger.info(f"[SCHEDULER] Total number of tasks is {sum_q_size}")

        # Set proportions of workers equal to the proportion of queue size.
        for q_type in q_sizes:
            ratio = q_sizes[q_type] / sum_q_size
            new_worker_map[q_type] = min(
                int(math.floor(ratio * max_workers)), q_sizes[q_type]
            )

        # CLEANUP: Assign the difference here to any random worker. Should be small.
        # logger.debug("Temporary new worker map: {}".format(new_worker_map))

        # Check the difference
        tmp_sum_q_size = sum(new_worker_map.values())
        difference = 0
        if sum_q_size > tmp_sum_q_size:
            difference = min(max_workers - tmp_sum_q_size, sum_q_size - tmp_sum_q_size)
        logger.debug(f"[SCHEDULER] Offset difference: {difference}")
        logger.debug(f"[SCHEDULER] Queue Types: {q_types}")

        if len(q_types) > 0:
            while difference > 0:
                win_q = random.choice(q_types)
                if q_sizes[win_q] > new_worker_map[win_q]:
                    new_worker_map[win_q] += 1
                    difference -= 1

    return new_worker_map
