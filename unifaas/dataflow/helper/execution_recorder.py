import subprocess
import os
import csv
import pathlib
import logging
import sqlite3
import sqlalchemy
import threading
from queue import Queue
import time
from datetime import datetime


UNIFAAS_HOME = os.path.join(pathlib.Path.home(), ".unifaas")

logger = logging.getLogger("unifaas")

class ExecutionRecorder:
    """
    Record the execution (runtime) info for each task.
    Analysis the data to do execution time prediction.
    """

    def __init__(self, record_dir=None):
        # Parsl home directory ~/.unifaas
        self._UNIFAAS_HOME =  os.path.join(pathlib.Path.home(), ".unifaas")
        if record_dir is None:
            self._RECORD_DIR = self._UNIFAAS_HOME
        self.database= os.path.join(self._RECORD_DIR, "execution_history.db") #store all function execution record
        self.table_name = "execution_history"
        self.record_format =  ["func_name", "input_size", "mem_avaliable", "cpu_percent", \
            "cpu_cores", "cpu_freqs_max","mem_usage","execution_time","output_size","insert_time","predict_time"]
        self.distinct_key = "func_name"
        self._init_execution_record_dir()
        self.sql_queue = Queue()
        self.write_record_thread = threading.Thread(target=self.writer_record_periodically, args=())
        self._kill_event = threading.Event()
        self.write_record_thread.daemon = True
        self.write_record_thread.start()

    def kill_writer(self):
        self.sql_queue.put("kill")
        self.write_record_thread.join()
        

    def writer_record_periodically(self):
        kill_flag = False
        while not self._kill_event.is_set():
            conn = None
            time.sleep(5)
            if not self.sql_queue.empty():
                conn = sqlite3.connect(self.database, check_same_thread=False)
                cursor = conn.cursor()
            cur_qsize = self.sql_queue.qsize()
            while not self.sql_queue.empty() and conn is not None:
                sql = self.sql_queue.get()
                if sql == "kill":
                    self._kill_event.set()
                    break
                cursor.execute(sql)
                conn.commit()
            if cur_qsize > 0:
                logger.info(f"[Recorder] Write {cur_qsize} execution records to database")
        logger.info(f"[Recorder] ExecutionRecorder is killed")

    def _create_table_sql(self):
        record_format = self.record_format
        sql = f"CREATE TABLE  {self.table_name} \n (ID INTEGER PRIMARY KEY AUTOINCREMENT   NOT NULL, \n"
        for i in range(len(record_format)):
            col = record_format[i]
            if col == "func_name":
                sql += f"{col} TEXT KEY NOT NULL"
            elif col == "insert_time":
                sql += f"{col} TEXT NOT NULL"
            else:
                sql += f"{col} NUMERIC NOT NULL"
            sql += ");" if i == len(record_format) - 1 else ", \n"
        return sql

    def _insert_sql(self, info_list):
        record_format = self.record_format
        sql = f"INSERT INTO {self.table_name} ("
        for i in range(len(record_format)):
            col = record_format[i]
            sql += f"{col}"
            sql += ")" if i == len(record_format) - 1 else ", "
        sql += " VALUES ("
        for i in range(len(info_list)):
            info = info_list[i]
            sql += f"'{info}'" if type(info) == str else f"{info}"
            sql += ")" if i == len(record_format) - 1 else ", "
        return sql

    def _init_execution_record_dir(self):
        """Initialize the execution record directory"""
        if not os.path.exists(self._UNIFAAS_HOME):
            logger.info(f"[Recorder] There is no record directory, create one \
                {self._RECORD_DIR}")
            os.makedirs(name=self._UNIFAAS_HOME, exist_ok=True, mode=0o777)       
        if not os.path.exists(self._RECORD_DIR):
            os.makedirs(name=self._RECORD_DIR, exist_ok=True, mode=0o777)
        if not os.path.exists(self.database):
            with open(self.database, "w") as f:
                pass
            conn = sqlite3.connect(self.database, check_same_thread=False)
            cursor = conn.cursor()
            sql = self._create_table_sql()
            cursor.execute(sql)
            conn.commit()

        
    def write_record(self,task_id, result):
        """Write the execution record to the execution record directory"""
        if 'cpu_freqs_max' in result.keys() and 'cpu_freqs_min' in result.keys() and 'cpu_freqs_current' in result.keys():
            cpu_max = max(result['cpu_freqs_max'], result['cpu_freqs_min'], result['cpu_freqs_current'])
            result['cpu_freqs_max'] = cpu_max
            result['insert_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            info_list = [result.get(s) for s in self.record_format]
            sql = self._insert_sql(info_list)
            self.sql_queue.put(sql)
        except Exception as e:
            logger.warn(f"[Recorder] Write execution record failed: {e}")

    def get_all_distinct_func(self):
        """Get all function name from the execution record directory"""
        conn = sqlite3.connect(self.database, check_same_thread=False)
        cursor = conn.cursor()
        sql = f"SELECT DISTINCT {self.distinct_key} FROM {self.table_name}"
        cursor.execute(sql)
        result = cursor.fetchall()
        func_list = [r[0] for r in result]
        return func_list

    def get_record_by_func(self, func_name):
        """Get all execution record of a function"""
        conn = sqlite3.connect(self.database, check_same_thread=False)
        cursor = conn.cursor()
        sql = f"SELECT * FROM {self.table_name} WHERE func_name = '{func_name}'"
        cursor.execute(sql)
        result = cursor.fetchall()
        return result


    def select_record_for_cpu_combination(self):
        conn = sqlite3.connect(self.database, check_same_thread=False)
        cursor = conn.cursor()
   
        cursor.execute("SELECT DISTINCT func_name FROM execution_history")
        func_names = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT DISTINCT cpu_cores, cpu_freqs_max FROM execution_history")
        cpu_combinations = cursor.fetchall()

        data = {}
        for cpu_combination in cpu_combinations:
            cores = int(cpu_combination[0])
            freq = int(cpu_combination[1])
            cpu_str = f"{cores}@{freq}"
            data[cpu_str] = {}
            for func_name in func_names:
                cursor.execute(
                    "SELECT * FROM execution_history WHERE cpu_cores=? AND cpu_freqs_max>=? AND cpu_freqs_max<=? AND func_name=?",
                    (cpu_combination[0], freq, freq+1, func_name)
                )
                data[cpu_str][func_name] = cursor.fetchall()

        cursor.close()
        conn.close()
        return data



class TransferRecorder(ExecutionRecorder):
    """
    Record the transfer (runtime) info for each task.
    Analysis the data to do transfer time prediction.
    """

    def __init__(self, record_dir=None):
        self._UNIFAAS_HOME =  os.path.join(pathlib.Path.home(), ".unifaas")
        if record_dir is None:
            self._RECORD_DIR = self._UNIFAAS_HOME
        else:
            self._RECORD_DIR = os.path.join(self._UNIFAAS_HOME, "transfer_record")
        self.record_format = ["src_address", "dest_address", "speed", "size", "fly_time", "total_time","prediction_time"]
        self.database = os.path.join(self._RECORD_DIR, "transfer_history.db")
        self.table_name = "transfer_history"
        self._init_db()
        self.sql_queue = Queue()
        self.write_record_thread = threading.Thread(target=self.writer_record_periodically, args=())
        self.write_record_thread.daemon = True
        self.write_record_thread.start()

    def writer_record_periodically(self):
        while True:
            kill_flag = False
            conn = None
            if not self.sql_queue.empty():
                conn = sqlite3.connect(self.database, check_same_thread=False)
                cursor = conn.cursor()
            cur_qsize = self.sql_queue.qsize()
            while not self.sql_queue.empty() and conn is not None:
                sql = self.sql_queue.get()
                cursor.execute(sql)
                conn.commit()
            if cur_qsize > 0:
                logger.info(f"[Recorder] Write {cur_qsize} transfer records to database")
            time.sleep(5)
    

    def _create_table_sql(self):
        record_format = self.record_format
        sql = f"CREATE TABLE  {self.table_name} \n (ID INTEGER PRIMARY KEY AUTOINCREMENT   NOT NULL, \n"
        for i in range(len(record_format)):
            col = record_format[i]
            if col == "src_address" or col == "dest_address":
                sql += f"{col} TEXT NOT NULL"
            else:
                sql += f"{col} NUMERIC NOT NULL"
            sql += ");" if i == len(record_format) - 1 else ", \n"
        return sql

    def _init_db(self):
        if not os.path.exists(self.database):
            if not os.path.exists(self._UNIFAAS_HOME):
                os.makedirs(name=self._UNIFAAS_HOME, exist_ok=True, mode=0o777)
            with open(self.database, "w") as f:
                pass
            conn = sqlite3.connect(self.database, check_same_thread=False)
            cursor = conn.cursor()
            sql = self._create_table_sql()
            cursor.execute(sql)
            conn.commit()


    def write_record(self,src_address, dest_address, speed, size, fly_time, total_time,prediction_time):
        try:
            info_list = [src_address, dest_address, speed, size, fly_time, total_time,prediction_time]
            sql = self._insert_sql(info_list)
            self.sql_queue.put(sql)
        except Exception as e:
            logger.warn(f"[Recorder] Write execution record failed: {e}")
        


def write_workflow_prediction_result(file_path, info):
    # if there is already a file, remove it and create a new one
    info_str = ""
    with open(file_path, "a") as f:
        for i in range(len(info) - 1):
            info_str += f"{info[i]},"
        if isinstance(info[-1],dict):
            for key, value in info[-1].items():
                info_str += f"{key}|{value}|"
            info_str += "\n"
        f.write(info_str)

