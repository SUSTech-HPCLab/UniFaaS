# Modified from: https://github.com/mrocklin/heft

from functools import partial
from itertools import chain
from collections import namedtuple
import logging
import re

exp_logger = logging.getLogger("experiment")

Event = namedtuple('Event', 'task start end')

compute_cost = {}
comm_cost = {}

rank_mem = {}
cbar_mem = {}
earlist_finish_times = {}
init_comm_cost = {}
id_to_fu = {}

def reverse_dict(d):
    result = {}
    for key in d:
        for val in d[key]:
            result[val] = result.get(val, tuple()) + (key,)
    return result


def wbar(ni, resources, compcost,worker_nums_stat):
    """ Average computation cost for each task in each endpoint"""
    tot_sum = 0
    for k in worker_nums_stat.keys():
        tot_sum += compcost(ni, k) * worker_nums_stat[k]
    return tot_sum / len(resources)


def cbar(ni, nj, resources, commcost,worker_nums_stat):
    """ Average communication cost """
    n = len(resources)
    if n == 1:
        return 0
    npairs = n * (n - 1)
    tot_sum = 0
    for k1 in worker_nums_stat.keys():
        for k2 in worker_nums_stat.keys():
            if k1 != k2:
                tot_sum += commcost(ni, nj, k1, k2) * worker_nums_stat[k1] * worker_nums_stat[k2]
            else:
                tot_sum += commcost(ni, nj, k1, k2) * worker_nums_stat[k1] * (worker_nums_stat[k1] - 1)
    # tot_sum2 = sum(commcost(ni, nj, a1, a2) for a1 in resources for a2 in resources
    #                 if a1 != a2)
    return 1. * tot_sum / npairs


def ranku(ni, resources, succ, compcost, commcost,worker_nums_stat):
    rank_res = rank_mem.get(ni, None)
    if rank_res is not None:
        return rank_res
    """ Rank_upward of task """
    rank = partial(ranku, compcost=compcost, commcost=commcost,
                   succ=succ, resources=resources,worker_nums_stat=worker_nums_stat)
    w = partial(wbar, compcost=compcost, resources=resources,worker_nums_stat=worker_nums_stat)
    c = partial(cbar, resources=resources, commcost=commcost,worker_nums_stat=worker_nums_stat)

    if ni in succ and succ[ni]:
        rank_mem[ni] = w(ni) + max(c(ni, nj) + rank(nj) for nj in succ[ni])
        return rank_mem[ni]
    else:
        rank_mem[ni] = w(ni)
        return rank_mem[ni]


def generate_core_id(endpoint, id):
    return f"{endpoint}_core{id}"

def parse_endpoint(endpoint_with_core_id):
    x = re.search("core\d+$", endpoint_with_core_id)
    if x is None:
        return endpoint_with_core_id
    else:
        return endpoint_with_core_id[:x.start()-1]

def parse_multicores_resource(resource, resource_poller):
    resource_status = resource_poller.get_real_time_status()
    worker_nums = {}
    for key in resource_status:
        worker_nums[key] = resource_status[key]['total_workers']
    core_resources = []
    for endpoint in resource:
        for i in range(worker_nums[endpoint]):
            core_resources.append(generate_core_id(endpoint, i))
    return core_resources, worker_nums


def heft_schedule_entry(dag, resources, appfu_to_task, execution_predictor, transfer_predictor, resource_poller):
    """ Schedule weight dag onto resources

    inputs:

    dag - weight DAG of tasks {a: (b, c)} where b, and c follow a
    resources - set of resources (funcX endpoint)
    compcost - function :: task, endpoint -> computation cost
    commcost - function :: task1, task2, ep1 , ep2 -> communication cost
    """
    import time
    start_time = time.time()
    core_resources,worker_nums = parse_multicores_resource(resources, resource_poller)
    for task_fu in dag:
        task_def = appfu_to_task[task_fu]
        id_to_fu[task_def['id']] = task_fu
        for x in dag[task_fu]:
            id_to_fu[appfu_to_task[x]['id']] = x
    
    for fu in appfu_to_task:
        task_def = appfu_to_task[fu]
        if 'predict_output' not in task_def.keys():
            execution_predictor.predict(task_def)
        else:
            pass

 
    computcost_with_predictor = partial(computcost, predictor=execution_predictor, fu_to_task=appfu_to_task)
    commcost_with_predictor = partial(commcost, predictor=transfer_predictor, fu_to_task=appfu_to_task, execution_predictor=execution_predictor)

    rank = partial(ranku, resources=core_resources, succ=dag,
                   compcost=computcost_with_predictor, commcost=commcost_with_predictor,worker_nums_stat=worker_nums)

    prec = reverse_dict(dag)

    tasks = set(dag.keys()) | set(x for xx in dag.values() for x in xx)
    exp_logger.info(f"HEFT schedule {len(tasks)} tasks totally")
    tasks = sorted(tasks, key=rank)

    end_time = time.time()
    exp_logger.info(f"HEFT schedule count rank: {end_time - start_time}")
    allocate_res = dict()
    # dependency should be checked
    cnt = 0
    init_commcost_before_allocate(tasks, prec, core_resources, transfer_predictor)
    for task in reversed(tasks):
        allocate(task, orders, allocate_res, prec, computcost_with_predictor, commcost_with_predictor, transfer_predictor)
        cnt += 1
        if cnt % 100 == 0:
            exp_logger.info(f"allocated {cnt} tasks")
    
    for key in allocate_res.keys():
        allocate_res[key] = parse_endpoint(allocate_res[key])
    
    return orders, allocate_res

def init_commcost_before_allocate(tasks, prec, machines, transfer_predictor):
    machine_set = set()
    for m in machines:
        ep = parse_endpoint(m)
        machine_set.add(ep)


    for task in reversed(tasks):
        if task not in prec:
            init_comm_cost[task] = {}
            for m in machine_set:
                task_record = task.task_def
                init_comm_cost[task][m] = transfer_predictor.predict_init_comm_cost(task_record, m)


def find_first_gap(agent_orders, desired_start_time, duration):
    """Find the first gap in an agent's list of tasks
    The gap must be after `desired_start_time` and of length at least
    `duration`.
    """

    # No tasks: can fit it in whenever the task is ready to run
    if (agent_orders is None) or (len(agent_orders)) == 0:
        return desired_start_time

    # Try to fit it in between each pair of Events, but first prepend a
    # dummy Event which ends at time 0 to check for gaps before any real
    # Event starts.
    a = chain([Event(None, None, 0)], agent_orders[:-1])
    for e1, e2 in zip(a, agent_orders):
        earliest_start = max(desired_start_time, e1.end)
        if e2.start - earliest_start > duration+10:
            return earliest_start

    # No gaps found: put it at the end, or whenever the task is ready
    return max(agent_orders[-1].end+10, desired_start_time)


def endtime(task, events):
    """ Endtime of task in list of events """
    for e in events:
        if e.task == task:
            return e.end



def start_time(task, orders, allocate_res, prec, commcost, compcost, ep, transfer_predictor):
    """ Earliest time that task can be executed on ep """

    duration = compcost(task, ep)

    if task in prec:
        for p in prec[task]:
            if p not in allocate_res:
                exp_logger.error(f"task {p} not in allocate_res")

        comm_ready = max([ earlist_finish_times[p].end
                          + commcost(p, task, allocate_res[p], ep) for p in prec[task]])
    else:
        real_ep = parse_endpoint(ep)
        comm_ready = init_comm_cost[task][real_ep]

    return find_first_gap(orders[ep], comm_ready, duration)


def allocate(task, orders, allocate_res, prec, compcost, commcost, transfer_predictor):
    """ Allocate task to the machine with earliest finish time

    Operates in place
    """

    st = partial(start_time, task=task, orders=orders, allocate_res=allocate_res, prec=prec, commcost=commcost, 
        compcost=compcost, transfer_predictor=transfer_predictor)

    def ft(machine):
        return st(ep=machine) + compcost(task, machine)

    earlist_time = float("inf")
    target_ep = None
    for key in orders.keys():
        if ft(key) < earlist_time:
            earlist_time = ft(key)
            target_ep = key

    start = st(ep=target_ep)
    end = earlist_time

    event_ending = Event(task, start, end)
    orders[target_ep].append(event_ending)
    orders[target_ep] = sorted(orders[target_ep], key=lambda e: e.start)

    allocate_res[task] = target_ep
    earlist_finish_times[task] = event_ending


def makespan(orders):
    """ Finish time of last task """
    return max(v[-1].end for v in orders.values() if v)



def computcost(task, ep, predictor, fu_to_task):
    task_record = fu_to_task[task]
    ep = parse_endpoint(ep)
    if 'predict_execution' not in task_record.keys():
        predictor.predict(task_record)
    if ep in task_record['predict_execution'].keys():
        return task_record['predict_execution'][ep]
    else:
        return 0


def commcost(task_i, task_j, ep_m, ep_n, predictor, fu_to_task, execution_predictor):
    ep_m = parse_endpoint(ep_m)
    ep_n = parse_endpoint(ep_n)
    if ep_m == ep_n:
        return 0 
    else:
        task_record_i = fu_to_task[task_i]
        if 'predict_output' not in task_record_i.keys():
            (func_name, input_size, predict_execution, predict_output) = execution_predictor.predict(task_record_i)
        return predictor.predict_comm_cost(fu_to_task[task_i], ep_m, ep_n)
    
