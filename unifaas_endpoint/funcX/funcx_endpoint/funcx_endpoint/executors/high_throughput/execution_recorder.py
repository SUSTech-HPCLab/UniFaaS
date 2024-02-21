import os
import pathlib
import csv

class ExecutionRecorder:

    def __init__(self):
        self._FUNCX_HOME = os.path.join(pathlib.Path.home(), ".funcx")
        self._FUNCX_EXECUTION_RECORD = os.path.join(self._FUNCX_HOME, "execution_record")
        self.doing_record = True
        self.max_files = 100
        self._init_execution_record_dir()
        self.latest_record_file = self._get_record_file()

    def _init_execution_record_dir(self):
        """Initialize the execution record directory"""
        
        if not os.path.exists(self._FUNCX_EXECUTION_RECORD):
            os.makedirs(name=self._FUNCX_EXECUTION_RECORD, exist_ok=True, mode=0o777)


    def _is_empty_record(self, dir_path):
        """Check if the record directory is empty"""
        return not os.listdir(dir_path)

    def _get_record_file(self):
        """Get the file which will be written a record """

        # If there is no record file, create a new one
        if self._is_empty_record(self._FUNCX_EXECUTION_RECORD):
            number = 0
            file_path = os.path.join(self._FUNCX_EXECUTION_RECORD, "%08d" % number)
            with open(file_path, mode="w") as f:
                pass
            return file_path
        
        # If there are record files, get the last one
        file_list = os.listdir(self._FUNCX_EXECUTION_RECORD)
        file_list.sort(reverse=True)
        return os.path.join(self._FUNCX_EXECUTION_RECORD, file_list[0])
        

    def _create_new_record_file(self):
        """
         Here should be a strategies like RLU, to update the record
        """
        old_file = os.path.basename(self.latest_record_file)
        new_name = int(self.latest_record_file) + 1
        if new_name > self.max_files:
            self.doing_record = False
            return
        new_file_path = os.path.join(self._FUNCX_EXECUTION_RECORD, "%08d" % new_name)
        with open(new_file_path, mode="w") as f:
                pass
        self.latest_record_file = new_file_path

            
         
    def write_record(self,task_id, result):
        """Write the execution record to the execution record directory"""

        if self.doing_record : 
            # information be append to a record file, so the file should be opened in append mode
                
            with open(self.latest_record_file, "a") as f:
                info_list = [task_id, result.get("func_name"), result.get("input_size"),
                            result.get("mem_avaliable"), result.get("cpu_percent"),
                            result.get("execution_time"), result.get("mem_usage")]
                f = csv.writer(f, quoting=csv.QUOTE_NONE)
                f.writerow(info_list)

            # get the size of record file
            file_size = os.stat(self.latest_record_file).st_size / (1024*1024)

            # file_size > 10mb, create a new one
            if file_size > 10:
                self._create_new_record_file()

        