import socketserver
import logging
import threading
import queue
import paramiko
import time
import uuid
from functools import partial
from funcx import set_file_logger
import threading
import os

class SFTPConnection():

    def __init__(self, address, username, password, port, sftp_client,is_available=True):
        self.address = address
        self.username = username
        self.password = password
        self.port = port
        self.is_available = is_available
        self.sftp_client, self.ssh = self._create_sftp_client()

    def _create_sftp_client(self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.address, username=self.username, password=self.password, port=self.port)
        return ssh.open_sftp(), ssh
    
    def set_status(self, status):
        self.is_available = status
    
    def close(self, sftp_session):
        # sftp_session.close()
        # close means session is over
        # However current connection is available to create a new session
        self.set_status(True)

    def get_status(self):
        return self.is_available
    
    def get_sftp(self):
        self.set_status(False)
        return self.sftp_client
        
    def check_and_recovery(self):
        transport = self.ssh.get_transport()
        if not transport.is_active():
            self.ssh.connect(self.address, username=self.username, password=self.password, port=self.port)
            self.sftp_client = self.ssh.open_sftp()



class SFTPAgent():

    def __init__(self, config=None, logger=None):
        self.logger = logger
        self.config = config 
        self.connection_pool = []
        self.init_connection_pool(config["address"], config["port"], config["username"], 
        config["password"], max_connections=config["num_of_connections"]) # can use another way to auth
        self.lock = threading.Lock()  
        self.transfer_items = {}
        self.check_thread = threading.Thread(target=self.check_active_connection_and_recovery)
        self.check_thread.daemon = True
        self.check_thread.start()

    
    def _transfer_callback(self,transferred: int, tobe_transferred: int, task_id, start_time):
        if transferred == tobe_transferred:
            self.transfer_items[task_id]['transfer_time'] = time.time() - start_time
            self.transfer_items[task_id]['transfer_size'] = transferred
            return

    def transfer(self,src_adr, src_loc, dest_adr, dest_loc, task_id):
        self.transfer_items[task_id] = {}
        if src_loc == dest_loc:
            self.transfer_items[task_id]['transfer_time'] = 0
            self.transfer_items[task_id]['transfer_size'] = 0
            return self.transfer_items[task_id]
        sftp_con = self.find_available_connection()
        with self.lock:
            sftp = sftp_con.get_sftp()
        # catch exception
        try:
            transfer_start_time = time.time()
            partial_callback = partial(self._transfer_callback, task_id=task_id, start_time=transfer_start_time)
            sftp.put(src_loc, dest_loc, callback=partial_callback)
            transfer_end_time = time.time()
            with self.lock:
                sftp_con.close(sftp)
            self.logger.info(f"Transfer file from {src_adr}:{src_loc} to {dest_adr}:{dest_loc} finished in {transfer_end_time - transfer_start_time} seconds")
            return self.transfer_items[task_id]
        except Exception as e:
            sftp_con.check_and_recovery()                
            try:
                with self.lock:
                    sftp = sftp_con.get_sftp()
                transfer_start_time = time.time()
                partial_callback = partial(self._transfer_callback, task_id=task_id, start_time=transfer_start_time)
                sftp.put(src_loc, dest_loc, callback=partial_callback)
                transfer_end_time = time.time()
                with self.lock:
                    sftp_con.close(sftp)
                self.logger.info(f"Retry Transfer file from {src_adr}:{src_loc} to {dest_adr}:{dest_loc} finished in {transfer_end_time - transfer_start_time} seconds")
                return self.transfer_items[task_id]
            except Exception as e:
                pass

            with self.lock:
                sftp_con.set_status(True)
            self.logger.error(f"Failed to transfer file from {src_adr}:{src_loc}  to {dest_adr}:{dest_loc} with error: {e}")


    def init_connection_pool(self, dest_adr, 
    dest_port, username, password, max_connections=3):
        for i in range(max_connections):
            self.connection_pool.append(SFTPConnection(dest_adr, username, password,dest_port, True))


    def find_available_connection(self):
        start_time = time.time()
        while True:
            for i in range(len(self.connection_pool)):
                with self.lock:
                    if self.connection_pool[i].get_status():
                        # if not self.connection_pool[i].check_connection():
                        #     self.connection_pool[i].recovery()
                            
                        return self.connection_pool[i]
            time.sleep(0.5)
            if time.time() - start_time > 10000:
                raise Exception("No available connection")
        return None

    def check_active_connection_and_recovery(self):
        while True:
            # check every 5 mins
            time.sleep(300)
            for i in range(len(self.connection_pool)):
                self.connection_pool[i].check_and_recovery()


class EchoHandler(socketserver.BaseRequestHandler):
        
    def handle(self):
        import shutil
        import os
        import uuid
        data = self.request.recv(1024).decode("utf-8")
        if self.client_address[0] not in self.server.avaliable_address:
            return
        if data == "STOP":
            self.server.shutdown()
            self.server.server_close()
        elif data == "LIST":
            self.request.sendall(str(self.server.avaliable_address).encode())
        elif data.startswith("TRANSFER"):
            src_identifier = data.split("|")[1]  # user@ip
            src_loc =  data.split("|")[2]
            bak_basename =  os.path.basename(src_loc)+ str(uuid.uuid4())
            bak_loc = os.path.join(os.path.dirname(src_loc), bak_basename)
            shutil.copy(src_loc, bak_loc)
            src_loc = bak_loc
            dest_identifier =  data.split("|")[3]
            dest_loc =  data.split("|")[4]
            src_adr = src_identifier.split("@")[1]
            dest_adr = dest_identifier.split("@")[1]
            task_id = str(uuid.uuid4())
            try:
                result = self.server.SFTPAgent_map[dest_identifier].transfer(src_adr, src_loc, dest_adr, dest_loc, task_id)
                time_init = time.time()
                while task_id in self.server.SFTPAgent_map[dest_identifier].transfer_items and time.time() - time_init < 3:
                    if "transfer_time" in self.server.SFTPAgent_map[dest_identifier].transfer_items[task_id].keys():
                        break
                    time.sleep(0.1)
                self.request.sendall(f"SUCCESS|{task_id}|{result['transfer_time']}|{result['transfer_size']}".encode())
                if os.path.exists(src_loc):
                    os.remove(src_loc)
            except Exception as e:
                if os.path.exists(src_loc):
                    os.remove(src_loc)
                self.request.sendall(f"FAILED|{task_id}".encode())
                return

        


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    # Can't be deleted!
    pass

class TransferCommandServer():
    def __init__(self, logdir, host_ip, port, connected_endpoints_info):
        while True:
            try:
                self.logdir = logdir
                
                self.logger = set_file_logger(
                    os.path.join(self.logdir, "sftp.log"),
                    name="sftplogger",
                )
                self.server = ThreadedTCPServer((host_ip, port), EchoHandler)
                self.server.avaliable_address = []
                self._init_available_address(connected_endpoints_info)
                self.server.SFTPAgent_map = self._init_transfer_agent_map(connected_endpoints_info, self.logger)
                self.logger.info(f"Transfer command server started at {host_ip}:{port}")
                self.server_thread = threading.Thread(target=self.server.serve_forever,
                                                        name="Transfer-Command-Server")
                self.server_thread.daemon = True
                self.server_thread.start()
                break
            except Exception as e:
                self.logger.info(f"Transfer command server failed to start with error: {e}")
                time.sleep(1)


    def _init_available_address(self, connected_endpoints_info):
        """
        format of connected_endpoints_info:
        { "ep_name" : (ip, port, username, password)}
        """ 
        for info in connected_endpoints_info.values():
            self.server.avaliable_address.append(info[0])

    def _init_transfer_agent_map(self, connected_endpoints_info, logger):
        agent_map = {}
        for key in connected_endpoints_info.keys():
            info_pair = connected_endpoints_info[key]
            agent_identifier = f"{info_pair[2]}@{info_pair[0]}"
            config = {
                "address":info_pair[0],
                "port":info_pair[1],
                "username": info_pair[2],
                "password": info_pair[3],
                "num_of_connections": info_pair[4],
            }
            agent_map[agent_identifier] = SFTPAgent(config=config, logger=logger)
        return agent_map
        