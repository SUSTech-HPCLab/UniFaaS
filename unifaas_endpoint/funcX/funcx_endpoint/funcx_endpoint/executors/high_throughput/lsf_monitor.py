import subprocess
import os
import collections


class LSFMonitor:

    def __init__(self):
        self.hosts_info = self.get_hosts_info()

    @staticmethod
    def execute_cmd(cmd):
        current_env = os.environ.copy()
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=current_env,
            shell=True)
        (stdout, stderr) = proc.communicate()
        retcode = proc.returncode
        return retcode, stdout.decode('utf-8'), stderr.decode('utf-8')

    def get_hosts_info(self, cmd='lsload -w'):
        """ example
        HOST_NAME               status  r15s   r1m  r15m   ut    pg  ls    it   tmp   swp   mem
        r02n09                      ok   0.0   0.0   9.4   0%   0.0   0 17344  212G  1.5G 160.7G
        r03n29                      ok   0.0   0.0   0.0   0%   0.0   0  1578  212G  3.1G  163G
        """
        retcode, cmd_out, cmd_err = self.execute_cmd(cmd)
        if retcode != 0 or cmd_err != '':
            raise Exception(f"[LSF Monitor] Failed to get host information: {cmd_err}")

        line_no = 0
        host_info = collections.OrderedDict()
        key_list = []
        for line in cmd_out.split('\n'):
            if line_no == 0:
                key_list = line.split()
                for key in key_list:
                    host_info[key] = []
            else:
                info_entry = line.split()
                if 'unavail' in line or len(info_entry) != len(key_list):
                    continue
                for i in range(len(key_list)):
                    host_info[key_list[i]].append(info_entry[i])
            line_no += 1
        return host_info

    def get_queues_info(self, cmd='bqueues -w'):
        """ Example       NJOBS: the sum of pending, running, sups jobs' CPU cores
        QUEUE_NAME      PRIO STATUS          MAX JL/U JL/P JL/H NJOBS  PEND   RUN  SUSP  RSV PJOBS
        debug            40  Open:Active       -   80    -    -    80     0    80     0    0     0
        medium           30  Open:Active       -    -    -    - 10236     0 10236     0    0     0
        ser              30  Open:Active       -    -    -    -  1813   600  1213     0    0    21
        """
        retcode, cmd_out, cmd_err = self.execute_cmd(cmd)
        if retcode != 0 or cmd_err != '':
            raise Exception(f"[LSF Monitor] Failed to get queues information: {cmd_err}")
        line_no = 0
        queue_info = collections.OrderedDict()
        key_list = []
        njobs_no = -1
        for line in cmd_out.split('\n'):
            if line_no == 0:
                key_list = line.split()
                for i in range(len(key_list)):
                    key = key_list[i]
                    if key == 'NJOBS':
                        njobs_no = i
                    queue_info[key] = []
            else:
                info_entry = line.split()
                # only store the open active queues
                if 'Open:Active' not in line or len(info_entry) != len(key_list):
                    continue
                # if there is no submit history on this queue, skip it.
                if info_entry[njobs_no] == '0':
                    continue
                for i in range(len(key_list)):
                    queue_info[key_list[i]].append(info_entry[i])
            line_no += 1
        return queue_info
