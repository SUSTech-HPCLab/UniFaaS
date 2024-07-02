from unifaas.executors.funcx.executor import FuncXExecutor
from unifaas.dataflow.helper.execution_recorder import TransferRecorder
from unifaas.dataflow.helper.resource_status_poller import ResourceStatusPoller
from funcx.sdk.file import RemoteFile, RemoteDirectory, RsyncFile, GlobusFile
from fair_research_login import JSONTokenStorage, NativeClient
import logging
import subprocess
from functools import partial
import re
from queue import Queue

logger = logging.getLogger("unifaas")
exp_logger = logging.getLogger("experiment")
from concurrent.futures import ThreadPoolExecutor
import uuid
import time
import os
from socket import socket, AF_INET, SOCK_STREAM
import globus_sdk
import threading


class RsyncTransferClient:
    def __init__(
        self, executors, password_file=None, transfer_record_dir=None, passwordless=True
    ):
        if password_file == None:
            raise Exception("Can't find the password file")

        self.transfer_recorder = TransferRecorder(record_dir=transfer_record_dir)
        self.resource_poller = ResourceStatusPoller(executors=executors)
        self.executors = executors
        self.transfer_thread_pool = ThreadPoolExecutor(max_workers=10)
        self.password_file = (
            password_file  # password file: store the password for other endpoints
        )
        self.executor_address_info = {}
        # self.transfer_tasks = {}  # task_id: future object, task id is a random uuid
        self.transfer_tasks = Queue()
        self._gather_endpoints_address_info()
        self.passwordless = passwordless

    def _get_password_from_file(self, rsync_ip, rsync_username):
        import csv

        with open(self.password_file, "r") as f:
            pwd_reader = csv.reader(f, delimiter=",")
            for row in pwd_reader:
                if row[0] == rsync_ip and row[1] == rsync_username:
                    return row[2]
        return None

    def generate_transfer_command(
        self, src_ip, src_user, dest_ip, dest_user, src_path, dest_path
    ):
        h1_password = self._get_password_from_file(src_ip, src_user)
        h2_password = self._get_password_from_file(dest_ip, dest_user)
        h1_address = f"{src_user}@{src_ip}"
        h2_address = f"{dest_user}@{dest_ip}"
        cmd = f'sshpass -p "{h1_password}" ssh {h1_address} "sshpass -p "{h2_password}" rsync -av {src_path}  {h2_address}:{dest_path} " '
        return cmd

    def generate_passwordless_command(
        self, src_ip, src_user, dest_ip, dest_user, src_path, dest_path
    ):
        h1_address = f"{src_user}@{src_ip}"
        h2_address = f"{dest_user}@{dest_ip}"
        cmd = f'ssh {h1_address} " rsync -av {src_path}  {h2_address}:{dest_path} " '
        return cmd

    def _gather_endpoints_address_info(self):
        """
        gather the information of address.
        RsyncClient gather, ip,username, data_pathls
        GlobusClient gather the globus_id, data_path
        """
        for exe_key in self.executors.keys():
            # all info contains the idle workers/address info etc
            funcx_executor = self.executors[exe_key]
            if isinstance(funcx_executor, FuncXExecutor):
                info = self.resource_poller.get_resource_status_by_label(exe_key)
                data_path = info.get("local_data_path")
                rsync_username = info.get("rsync_username")
                rsync_ip = info.get("rsync_ip")
                self.executor_address_info[exe_key] = {
                    "address_id": f"{rsync_username}@{rsync_ip}",
                    "data_path": data_path,
                    "rsync_username": rsync_username,
                    "rsync_ip": rsync_ip,
                }

    def transfer(self, file, remote_host):
        if isinstance(file, RemoteFile) or isinstance(file, RemoteDirectory):
            recursive = True if isinstance(file, RemoteDirectory) else False
            rsync_ip = file.rsync_ip
            rsync_username = file.rsync_username
            check_rsync_auth = file.check_rsync_auth
            src_path = file.file_path
            basename = file.file_name
            # check whehter the remote host in the dictionary
            if remote_host not in self.executor_address_info.keys():
                raise Exception(
                    "Transfer error! Can't find the remote host at executor address dict"
                )
            dest_ip = self.executor_address_info[remote_host]["rsync_ip"]
            dest_user = self.executor_address_info[remote_host]["rsync_username"]
            dest_path = self.executor_address_info[remote_host]["data_path"]
            if self.passwordless:
                cmd = self.generate_passwordless_command(
                    rsync_ip, rsync_username, dest_ip, dest_user, src_path, dest_path
                )
            else:
                cmd = self.generate_transfer_command(
                    rsync_ip, rsync_username, dest_ip, dest_user, src_path, dest_path
                )
            task_id = str(uuid.uuid4())

            # check the same dest and source only support remotefile for now
            if isinstance(file, RemoteFile):
                basename = os.path.basename(src_path)
                src_dir = src_path[: -len(basename)]
                if (
                    dest_ip == rsync_ip
                    and dest_user == rsync_username
                    and src_dir == dest_path
                ):
                    task = {
                        "task_id": task_id,
                        "status": "LOCAL",
                        "future": None,
                        "stdout": None,
                        "stderr": None,
                    }
                    task["remote_file"] = file
                    task["src_ep"] = f"{rsync_username}@{rsync_ip}"
                    task["dst_ep"] = f"{dest_user}@{dest_ip}"
                    logger.info(f"[Rsync] Skip transfer task, due to local: {task}")
                    return task

            logger.debug(f"Transfering by the command: {cmd}")
            future = self.transfer_thread_pool.submit(
                subprocess.run,
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            task = {
                "task_id": task_id,
                "status": "ACTIVE",
                "future": future,
                "stdout": None,
                "stderr": None,
                "transfer_times": 0,
                "start_time": time.time(),
            }
            task["remote_file"] = file
            self.transfer_tasks.put(task)
            future.add_done_callback(partial(self._transfer_done, task=task))
            # aggregate the task info
            task["src_ep"] = f"{rsync_username}@{rsync_ip}"
            task["dst_ep"] = f"{dest_user}@{dest_ip}"
            task["cmd"] = cmd
            logger.info(f"[Rsync] transfer task: {task}")
            return task
        else:
            raise Exception("Transfer error! The file type is not supported")

    def _transfer_done(self, future, task):
        try:
            transfer_result = future.result()
            if transfer_result.returncode == 0:
                task["status"] = "SUCCEEDED"
                speed, transfer_size = self._analyze_std_out(transfer_result.stdout)
                task["speed"] = speed
                task["transfer_size"] = transfer_size
            else:
                task["status"] = "FAILED"

            if task["status"] == "FAILED":
                if task["transfer_times"] < 1:
                    task["transfer_times"] += 1
                    logger.info(f"[Rsync] Transfer failed, retrying {task}")
                    self._retry_transfer(task)
                    return
                logger.info(f"[Rsync] Transfer failed after retry {task}")
            task["stdout"] = transfer_result.stdout
            task["stderr"] = transfer_result.stderr
            logger.info(f"[Rsync] Transfer finished {task}")
            if task["status"] == "SUCCEEDED":
                speed = float(speed.replace(",", ""))
                transfer_size = float(transfer_size.replace(",", ""))

        except Exception as e:
            task["status"] = "FAILED"
            task["stdout"] = transfer_result.stdout
            task["stderr"] = transfer_result.stderr
            logger.info(f"Rsync transfer failed: {e}")
        return

    def _analyze_std_out(self, out_info):
        """
        analyze the stdout info, which is generated by the rsync transfer
        out_info: the stdout info bytes
        (speed_num, size_num) bytes/sec and bytes
        """
        out_info = out_info.decode("utf-8")  # raw out_info is in bytes format
        speed_pattern = (
            r"\d[^a-z]*bytes\/sec"  # search the string like 140.00 bytes/sec
        )
        if len(re.findall(speed_pattern, out_info)) != 1:
            raise Exception("Rsync transfer analyze failed! Can't find the speed info")
        speed_str = re.findall(speed_pattern, out_info)[0]
        speed_num = re.findall(r"[\d.,]+", speed_str)[
            0
        ]  # conatins comma, like 140,000.00
        size_pattern = r"total size is [\d.,]+"
        size_str = re.findall(size_pattern, out_info)[0]
        size_num = re.findall(r"[\d.,]+", size_str)[0]
        return (speed_num, size_num)

    def _retry_transfer(self, task):
        """
        retry the transfer task
        task: the task dict
        """
        task_id = task["task_id"]
        if task["status"] == "FAILED":
            task["status"] = "RETRYING"
            cmd = task["cmd"]
            future = self.transfer_thread_pool.submit(
                subprocess.run,
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            task["future"] = future
            future.add_done_callback(partial(self._transfer_done, task=task))
            logger.info(f"[Rsync] Retry transfer task: {task}")
        else:
            raise Exception("Rsync retry transfer failed!")


class SFTPClient:
    def __init__(
        self,
        executors,
        command_server_port=12548,
        transfer_record_dir=None,
    ):
        self.transfer_recorder = TransferRecorder(record_dir=transfer_record_dir)
        self.resource_poller = ResourceStatusPoller(executors=executors)
        self.executors = executors
        # key: executor name, value: ThreadPoolExecutor
        exp_logger.info("Starting ThreadPoolExecutor for each transfer command")
        self.thread_pool = {}
        self.retry_submission = ThreadPoolExecutor(max_workers=1)
        self.transfer_cmd_queue_dict = {}
        self.cur_on_fly_trans = {}
        self.transferring_size = {}
        self.transferring_file_num = {}
       # self.transfer_history_tbl = {}
        for key in self.executors.keys():
            self.thread_pool[key] = {}
            self.transfer_cmd_queue_dict[key] = {}
            self.cur_on_fly_trans[key] = {}
            self.transferring_size[key] = {}
            self.transferring_file_num[key] = {}
            for key2 in self.executors.keys():
                self.thread_pool[key][key2] = ThreadPoolExecutor(max_workers=3)
                self.transfer_cmd_queue_dict[key][key2] = Queue()
                self.cur_on_fly_trans[key][key2] = 0
                self.transferring_size[key][key2] = 0
                self.transferring_file_num[key][key2] = 0

        self.command_server_port = (
            command_server_port  # command server on the endpoint. default is 12548
        )
        self.executor_address_info = {}
        self.transfer_tasks = Queue()

        self._gather_endpoints_address_info()

    def _gather_endpoints_address_info(self):
        """
        gather the information of address.
        RsyncClient gather, ip,username, data_pathls
        GlobusClient gather the globus_id, data_path
        """
        for exe_key in self.executors.keys():
            # all info contains the idle workers/address info etc
            funcx_executor = self.executors[exe_key]
            if isinstance(funcx_executor, FuncXExecutor):
                info = self.resource_poller.get_resource_status_by_label(exe_key)
                data_path = info.get("local_data_path")
                rsync_username = info.get("rsync_username")
                host_ip = info.get("rsync_ip")
                self.executor_address_info[exe_key] = {
                    "identifier": f"{rsync_username}@{host_ip}",
                    "data_path": data_path,
                    "username": rsync_username,
                    "host_ip": host_ip,
                }

    def generate_transfer_command(
        self, src_ip, src_user, dest_ip, dest_user, src_path, dest_path
    ):
        cmd = (
            f"TRANSFER|{src_user}@{src_ip}|{src_path}|{dest_user}@{dest_ip}|{dest_path}"
        )
        return cmd

    def send_transfer_command(self, cmd, remote_host, remote_port=12548):
        s = socket(AF_INET, SOCK_STREAM)
        s.connect((remote_host, remote_port))
        s.send(cmd.encode("utf-8"))
        recv_msg = s.recv(8192).decode("utf-8")
        return recv_msg

    def put_transfer_task_in_queue(self, task):
        src_host = task["src_host"]
        dest_host = task["dest_host"]
        self.transfer_cmd_queue_dict[src_host][dest_host].put(task)
        self.transferring_size[src_host][dest_host] += task["file_size"]
        self.transferring_file_num[src_host][dest_host] += 1
        if self.cur_on_fly_trans[src_host][dest_host] > 3:
            pass
        else:
            self.submit_one_task(src_host, dest_host)

    def submit_one_task(self, src_host, dest_host):
        if self.transfer_cmd_queue_dict[src_host][dest_host].empty():
            return
        task = self.transfer_cmd_queue_dict[src_host][dest_host].get()
        src_host = task["src_host"]
        dest_host = task["dest_host"]
        transfer_cmd = task["cmd"]
        src_ip = task["src_ip"]
        logger.info(f"[SFTP] transfer task: {task}")
        future = self.thread_pool[src_host][dest_host].submit(
            self.send_transfer_command, transfer_cmd, src_ip
        )
        task["future"] = future
        future.add_done_callback(partial(self._transfer_done, task=task))
        self.cur_on_fly_trans[src_host][dest_host] += 1

    def transfer(self, file, remote_host):
        if isinstance(file, RemoteFile) or isinstance(file, RemoteDirectory):
            recursive = True if isinstance(file, RemoteDirectory) else False
            src_ip = file.rsync_ip
            src_username = file.rsync_username
            check_rsync_auth = file.check_rsync_auth  # not used
            src_path = file.file_path
            basename = file.file_name
            # check whehter the remote host in the dictionary
            if remote_host not in self.executor_address_info.keys():
                raise Exception(
                    "Transfer error! Can't find the remote host at executor address dict"
                )
            dest_ip = self.executor_address_info[remote_host]["host_ip"]
            dest_user = self.executor_address_info[remote_host]["username"]
            dest_path = self.executor_address_info[remote_host]["data_path"]

            dest_path = (
                f"{dest_path}{basename}"
                if dest_path.endswith("/")
                else f"{dest_path}/{basename}"
            )

            transfer_cmd = self.generate_transfer_command(
                src_ip, src_username, dest_ip, dest_user, src_path, dest_path
            )
            task_id = str(uuid.uuid4())

            # check the same dest and source only support remotefile for now
            if isinstance(file, RemoteFile):
                if (
                    dest_ip == src_ip
                    and dest_user == dest_user
                    and src_path == dest_path
                ):
                    task = {
                        "task_id": task_id,
                        "status": "LOCAL",
                        "future": None,
                        "stdout": None,
                        "stderr": None,
                    }
                    task["remote_file"] = file
                    task["src_ep"] = f"{src_username}@{src_ip}"
                    task["dst_ep"] = f"{dest_user}@{dest_ip}"
                    task["dest_path"] = dest_path
                    logger.info(f"[SFTP] Skip transfer task, due to local: {task}")
                    # if task["remote_file"] not in self.transfer_history_tbl.keys():
                    #     self.transfer_history_tbl[task["remote_file"]] = {}
                    # if (
                    #     remote_host
                    #     not in self.transfer_history_tbl[task["remote_file"]]
                    # ):
                    #     self.transfer_history_tbl[task["remote_file"]][
                    #         remote_host
                    #     ] = task
                    return task
            identifier = f"{src_username}@{src_ip}"
            src_host = None

            for exe in self.executor_address_info.keys():
                if self.executor_address_info[exe]["identifier"] == identifier:
                    src_host = exe
                    break

            task = {
                "task_id": task_id,
                "status": "ACTIVE",
                "future": None,
                "stdout": None,
                "stderr": None,
                "transfer_times": 0,
                "start_time": time.time(),
            }
            task["remote_file"] = file
            task["src_ep"] = f"{src_username}@{src_ip}"
            task["dst_ep"] = f"{dest_user}@{dest_ip}"
            task["dest_path"] = dest_path
            task["src_host"] = src_host
            task["dest_host"] = remote_host
            task["src_ip"] = src_ip
            task["cmd"] = transfer_cmd
            task["file_size"] = file.file_size

            # if file in self.transfer_history_tbl.keys():
            #     if remote_host in self.transfer_history_tbl[file]:
            #         logger.info(f"[SFTP] Skip transfer task, due to already in history")
            #         exist_task = self.transfer_history_tbl[file][remote_host]
            #         if (
            #             exist_task["status"] == "SUCCEEDED"
            #             or exist_task["status"] == "LOCAL"
            #         ):
            #             task["status"] = "LOCAL"
            #             logger.info(
            #                 f"[SFTP] Skip transfer task, due to history marked with local: {task}"
            #             )
            #             return task
            #         return exist_task

            # if task["remote_file"] not in self.transfer_history_tbl.keys():
            #     self.transfer_history_tbl[task["remote_file"]] = {}
            # if remote_host not in self.transfer_history_tbl[task["remote_file"]]:
            #     self.transfer_history_tbl[task["remote_file"]][remote_host] = task

            self.put_transfer_task_in_queue(task)
            self.transfer_tasks.put(task)
            return task
        else:
            raise Exception("Transfer error! The file type is not supported")

    def get_transferring_size(self, src_host, dest_host):
        return self.transferring_size[src_host][dest_host]

    def get_transferring_file_num(self, src_host, dest_host):
        return self.transferring_file_num[src_host][dest_host]

    def _transfer_done(self, future, task):
        try:
            transfer_result = future.result()
            src_host = task["src_host"]
            dest_host = task["dest_host"]
            self.cur_on_fly_trans[src_host][dest_host] -= 1
            self.transferring_size[src_host][dest_host] -= task["file_size"]
            self.transferring_file_num[src_host][dest_host] -= 1
            if transfer_result.startswith("SUCCESS"):
                task["status"] = "SUCCEEDED"
                transfer_size = float(transfer_result.split("|")[-1])
                transfer_time = float(transfer_result.split("|")[-2])
                if transfer_time == 0:
                    transfer_time = 0.0001
                speed = transfer_size / transfer_time
                task["speed"] = speed
                task["fly_time"] = transfer_time
                task["transfer_size"] = transfer_size
                task["total_time"] = time.time() - task["start_time"]
                self.submit_one_task(src_host, dest_host)
                if "prediction_time" not in task.keys():
                    task["prediction_time"] = -1
                self.transfer_recorder.write_record(
                    task["src_ep"],
                    task["dst_ep"],
                    speed,
                    transfer_size,
                    transfer_time,
                    task["total_time"],
                    task["prediction_time"],
                )
            else:
                if task["transfer_times"] < 2:
                    task["transfer_times"] += 1
                    logger.info(f"[SFTP] Transfer failed, retrying {task}")
                    self._retry_transfer(task)
                    return
                else:
                    task["status"] = "FAILED"
                logger.info(f"[SFTP] Transfer failed after retry {task}")
            task["stdout"] = transfer_result
            task["stderr"] = transfer_result
            logger.info(f"[SFTP] Transfer finished {task}")
        except Exception as e:
            task["stdout"] = transfer_result
            task["stderr"] = transfer_result
            logger.info(f"[SFTP] transfer failed: {e}")
        return

    def _retry_transfer(self, task):
        task_id = task["task_id"]
        task["status"] = "RETRYING"
        transfer_cmd = task["cmd"]
        src_ip = transfer_cmd.split("|")[1].split("@")[1]
        future = self.retry_submission.submit(
            self.send_transfer_command, transfer_cmd, src_ip
        )
        task["future"] = future
        future.add_done_callback(partial(self._transfer_done, task=task))
        logger.info(f"[SFTP] Retry transfer task: {task}")


class GlobusTransferClient:
    """
    This GlobusTransferClient can submit transfer tasks from one endpoint to another.
    All communication with the Globus Auth and Globus Transfer services is enclosed
    in the Globus class. In particular, the Globus class is reponsible for:
     - managing an OAuth2 authorizer - getting access and refresh tokens,
       refreshing an access token, storing to and retrieving tokens from
       .globus.json file,
     - submitting file transfers,
     - monitoring transfers.
    """

    def __init__(
        self,
        executors,
        sync_level="checksum",
    ):
        """
        Initialize a globus transfer client
        """
        transfer_scope = "urn:globus:auth:scope:transfer.api.globus.org:all"
        scopes = [transfer_scope]
        CLIENT_ID = "a0cb81f7-757e-4564-ac93-7fef03368d53"
        TOKEN_LOC = os.path.expanduser("~/.unifaas/funcx_sdk_tokens.json")
        self.executors = executors

        self.native_client = NativeClient(
            client_id=CLIENT_ID,
            app_name="funcX data transfer",
            token_storage=JSONTokenStorage(TOKEN_LOC),
        )

        self.native_client.login(
            requested_scopes=scopes,
            no_local_server=True,
            no_browser=True,
            refresh_tokens=True,
        )

        transfer_authorizer = self.native_client.get_authorizers_by_scope(
            requested_scopes=scopes
        )[transfer_scope]

        self.transfer_client = globus_sdk.TransferClient(transfer_authorizer)
        self.sync_level = sync_level
        self.transfer_tasks = Queue()
        self.resource_poller = ResourceStatusPoller(executors=executors)

        self._kill_event = threading.Event()
        self.track_interval = 5
        self._track_globus_transfer_thread = threading.Thread(
            target=self._track_globus_transfer_status,
            args=(self._kill_event,),
            name="Track-Globus-Thread",
        )
        self._track_globus_transfer_thread.daemon = True
        self._track_globus_transfer_thread.start()

        self.executor_address_info = {}
        self.transferring_size = {}
        self.transferring_file_num = {}
        # self.transfer_history_tbl = {}
        self.flying_tasks = {}
        for key in self.executors.keys():
            self.transferring_size[key] = {}
            self.transferring_file_num[key] = {}
            for key2 in self.executors.keys():
                self.transferring_size[key][key2] = 0
                self.transferring_file_num[key][key2] = 0
        self._gather_endpoints_address_info()

    def transfer(self, file, remote_host):
        if isinstance(file, GlobusFile):
            dest_ep = self.executor_address_info[remote_host]["globus_ep_id"]
            dest_path = self.executor_address_info[remote_host]["data_path"]
            unifaas_transfer_task_id = str(uuid.uuid4())
            basename = file.file_name
            target_path = (
                f"{dest_path}{basename}"
                if dest_path.endswith("/")
                else f"{dest_path}/{basename}"
            )
            if dest_ep == file.endpoint:
                basename = file.file_name
                if target_path == file.file_path:
                    task = {
                        "task_id": unifaas_transfer_task_id,
                        "status": "LOCAL",
                        "future": None,
                        "stdout": None,
                        "stderr": None,
                    }
                    task["remote_file"] = file
                    task["src_ep"] = file.endpoint
                    task["dst_ep"] = dest_ep
                    task["dest_path"] = target_path
                    logger.info(f"Skip transfer task, due to local: {file}")
                    # if task["remote_file"] not in self.transfer_history_tbl.keys():
                    #     self.transfer_history_tbl[task["remote_file"]] = {}
                    # if (
                    #     remote_host
                    #     not in self.transfer_history_tbl[task["remote_file"]]
                    # ):
                    #     self.transfer_history_tbl[task["remote_file"]][
                    #         remote_host
                    #     ] = task
                    return task
            src_host = None
            for exe in self.executor_address_info.keys():
                if self.executor_address_info[exe]["globus_ep_id"] == file.endpoint:
                    src_host = exe
                    break
            task = {
                "task_id": unifaas_transfer_task_id,
                "status": "ACTIVE",
                "future": None,
                "stdout": None,
                "stderr": None,
                "transfer_times": 0,
                "start_time": time.time(),
            }
            task["remote_file"] = file
            task["src_ep"] = file.endpoint
            task["dst_ep"] = dest_ep
            task["dest_path"] = target_path
            task["src_host"] = src_host
            task["dest_host"] = remote_host
            task["file_size"] = file.file_size
            
            # if file in self.transfer_history_tbl.keys():
            #     if remote_host in self.transfer_history_tbl[file]:
            #         logger.info(
            #             f"[GLOBUS] Skip transfer task, due to already in history"
            #         )
            #         exist_task = self.transfer_history_tbl[file][remote_host]
            #         if (
            #             exist_task["status"] == "SUCCEEDED"
            #             or exist_task["status"] == "LOCAL"
            #         ):
            #             task["status"] = "LOCAL"
            #             logger.info(
            #                 f"[GLOBUS] Skip transfer task, due to history marked with local: {task}"
            #             )
            #             return task
            #         return exist_task
            # if task["remote_file"] not in self.transfer_history_tbl.keys():
            #     self.transfer_history_tbl[task["remote_file"]] = {}
            # if remote_host not in self.transfer_history_tbl[task["remote_file"]]:
            #     self.transfer_history_tbl[task["remote_file"]][remote_host] = task

            self.put_transfer_task_in_queue(task)
            globus_task = self.globus_transfer(file, dest_ep, target_path)
            self.flying_tasks[globus_task["task_id"]] = task
            task["future"] = globus_task
            self.transfer_tasks.put(task)
            return task
        else:
            raise Exception("This file type is not supported")
        pass

    def globus_transfer(
        self, transfer_file: RemoteFile, dest_ep, dest_path, recursive=False
    ):
        tdata = globus_sdk.TransferData(
            self.transfer_client,
            transfer_file.endpoint,
            dest_ep,
            label=f"Transfer on Globus Endpoint {dest_ep}",
            sync_level=self.sync_level,
        )
        tdata.add_item(transfer_file.file_path, dest_path, recursive=recursive)
        try:
            globus_transfer_task = self.transfer_client.submit_transfer(tdata)

        except Exception as e:
            print(
                "Globus transfer from {}{} to {}{} failed due to error: {}".format(
                    transfer_file.endpoint,
                    transfer_file.file_path,
                    dest_ep,
                    dest_path,
                    e,
                )
            )
            raise Exception(
                "Globus transfer from {}{} to {}{} failed due to error: {}".format(
                    transfer_file.endpoint,
                    transfer_file.file_path,
                    dest_ep,
                    dest_path,
                    e,
                )
            )
        return globus_transfer_task

    def status(self, task_id):
        status = self.transfer_client.get_task(task_id)
        return status

    def get_event(self, task_id):
        events = self.transfer_client.task_event_list(
            task_id, num_results=1, filter="is_error:1"
        )
        try:
            event = events.data[0]["details"]
            return event
        except IndexError:
            logger.debug(f"No globus transfer error for task {task_id}")
            return
        except Exception:
            logger.exception(
                "Got exception when fetching globus transfer "
                "error event for task {}".format(task_id)
            )
            return

    def cancel(self, task_id):
        res = self.transfer_client.cancel_task(task_id)
        logger.info(f"Canceling task {task_id}, got message: {res}")
        if res["code"] != "Canceled":
            logger.error(
                "Could not cancel task {}. Reason: {}".format(task_id, res["message"])
            )

    def _gather_endpoints_address_info(self):
        """
        gather the information of address.
        RsyncClient gather, ip,username, data_pathls
        GlobusClient gather the globus_id, data_path
        """
        for exe_key in self.executors.keys():
            # all info contains the idle workers/address info etc
            funcx_executor = self.executors[exe_key]
            if isinstance(funcx_executor, FuncXExecutor):
                info = self.resource_poller.get_resource_status_by_label(exe_key)
                data_path = info.get("local_data_path")
                globus_ep_id = info.get("globus_ep_id")
                self.executor_address_info[exe_key] = {
                    "data_path": data_path,
                    "globus_ep_id": globus_ep_id,
                }

    def put_transfer_task_in_queue(self, task):
        src_host = task["src_host"]
        dest_host = task["dest_host"]
        self.transferring_size[src_host][dest_host] += task["file_size"]
        self.transferring_file_num[src_host][dest_host] += 1

    def get_transferring_size(self, src_host, dest_host):
        return self.transferring_size[src_host][dest_host]

    def get_transferring_file_num(self, src_host, dest_host):
        return self.transferring_file_num[src_host][dest_host]

    def _track_globus_transfer_status(self, kill_event):
        while not kill_event.is_set():
            time.sleep(self.track_interval)

            for task_id, unifaas_transfer_task in list(self.flying_tasks.items()):
                transfer_status = self.status(task_id)
                if transfer_status["status"] == "SUCCEEDED":
                    src_host = unifaas_transfer_task["src_host"]
                    dest_host = unifaas_transfer_task["dest_host"]
                    self.transferring_size[src_host][
                        dest_host
                    ] -= unifaas_transfer_task["file_size"]
                    self.transferring_file_num[src_host][dest_host] -= 1
                    del self.flying_tasks[task_id]
                    # Since Globus not support showing the transfer speed, do not record the transfer for now.
                    unifaas_transfer_task["status"] = "SUCCEEDED"
                elif (
                    transfer_status["status"] == "FAILED"
                    or transfer_status["status"] == "INACTIVE"
                    or transfer_status["nice_status"] == "FILE_NOT_FOUND"
                ):
                    failed_reason = self.gtc.get_event(task_id)
                    if transfer_status["nice_status"] == "FILE_NOT_FOUND":
                        failed_reason = "FILE_NOT_FOUND"
                    self.gtc.cancel(task_id)
                    del self.flying_tasks[task_id]

                    if failed_reason is not None:
                        unifaas_transfer_task["status"] = "FAILED"
                    else:
                        if unifaas_transfer_task["transfer_times"] < 2:
                            unifaas_transfer_task["transfer_times"] += 1
                            self._retry_transfer(unifaas_transfer_task)
                        else:
                            unifaas_transfer_task["status"] = "FAILED"

    def _retry_transfer(self, unifaas_task):
        unifaas_task["status"] = "RETRYING"
        globus_task = self.globus_transfer(
            unifaas_task["remote_file"],
            unifaas_task["dst_ep"],
            unifaas_task["dest_path"],
        )
        self.flying_tasks[globus_task["task_id"]] = unifaas_task
        unifaas_task["future"] = globus_task
