from datetime import datetime, timedelta
from dataclasses import dataclass
import uuid  # 添加uuid模块导入

class TransferNode:
    def __init__(self, task_id, file_size, start_time, end_time):
        self.task_id = task_id
        self.file_size = file_size
        self.start_time = start_time
        self.end_time = end_time
        self.prev = None
        self.next = None

class TransferList:
    def __init__(self):
        self.head = None
        self.tail = None
        
    def append(self, node):
        if not self.head:
            self.head = node
            self.tail = node
        else:
            node.prev = self.tail
            self.tail.next = node
            self.tail = node
            
    def remove(self, node):
        if node.prev:
            node.prev.next = node.next
        else:
            self.head = node.next
            
        if node.next:
            node.next.prev = node.prev
        else:
            self.tail = node.prev
        node.prev = node.next = None




class MockTransferChannel:

    def __init__(self, src, dest, bandwidth, num_of_workers):
        self.src = src
        self.dest = dest
        self.bandwidth = bandwidth
        self.num_of_workers = num_of_workers
        # channels 放着上一个的task 直接假设完成
        self.channels = {}
        for i in range(self.num_of_workers):
            self.channels[i] = TransferList()
        self.task_map = {}  



    def _get_channel_available_time(self, channel, current_time):
        if not channel or not channel.tail:
            return current_time
        return max(current_time, channel.tail.end_time)

    def push_transfer_task(self, task_id, transfer_size, current_time):
        """
        添加新的传输任务
        Args:
            task_id: 任务ID
            transfer_size: 传输大小(bytes)
            current_time: 当前模拟时间
        Returns:
            预计完成时间
        """
        target_channel_id = 0
        earliest_time = current_time + timedelta(days=36500)
        for i in range(self.num_of_workers):
            available_time = self._get_channel_available_time(self.channels[i], current_time)
            if available_time < earliest_time:
                earliest_time = available_time
                target_channel_id = i
        
        channel = self.channels[target_channel_id]
        
        transfer_cost = transfer_size / (self.bandwidth * 1024 * 1024)
        start_time = self._get_channel_available_time(channel, current_time)
        end_time = start_time + timedelta(seconds=transfer_cost)
        
        new_node = TransferNode(
            task_id,
            transfer_size,
            start_time,
            end_time
        )
        
        channel.append(new_node)
        self.task_map[task_id] = (target_channel_id, new_node)
        
        return end_time

    def pop_task(self, task_id):
        """常数时间复杂度移除指定任务"""
        if task_id not in self.task_map:
            return None
            
        channel_id, node = self.task_map[task_id]
        self.channels[channel_id].remove(node)
        del self.task_map[task_id]
        return node

    def update_transfers(self, current_time, transferring_size, transferring_num):
        completed_tasks = []
        for channel_id, channel in self.channels.items():
            current = channel.head
            while current and current.end_time <= current_time:
                completed_tasks.append(current.task_id)
                transferring_size[self.src][self.dest] -= current.file_size
                if transferring_num[self.src][self.dest] >= 1:
                    transferring_num[self.src][self.dest] -= 1
                    
                next_node = current.next
                self.pop_task(current.task_id)
                current = next_node
                
        return completed_tasks
    
    
class MockDataManager:
    
    def __init__(self):
        self.bandwidth_info = {
            "EVA" : {"EVA": 1000, "taiyi":60 , "lab02":60, "cse_cluster":60, "data_pool":111, "qiming":60},
            "lab02" : {"EVA": 60, "taiyi":60, "lab02":1000,  "cse_cluster":60, "data_pool":110, "qiming":60},
            "cse_cluster" : {"EVA": 68.2, "taiyi":60 , "lab02":60,  "cse_cluster":1000, "data_pool":112, "qiming":60},
            "data_pool" : {"EVA": 70, "taiyi":1 , "lab02":1000,  "cse_cluster":110, "data_pool":1000, "qiming":1},
            "taiyi" : {"EVA": 40, "taiyi":1000 , "lab02":40,  "cse_cluster":45, "data_pool":1.5, "qiming":45},
            "qiming" : {"EVA": 60, "taiyi":60 , "lab02":45,  "cse_cluster":45, "data_pool":1.5, "qiming":1000},
        }

        self.channel_map = {}

    def init_mock_data_channel(self, src_ep, dest_ep, cur_transffering_size):
        if src_ep not in self.channel_map:
            self.channel_map[src_ep] = {} 
        if dest_ep not in self.channel_map[src_ep]:
            self.channel_map[src_ep][dest_ep] = MockTransferChannel(src_ep, dest_ep, self.bandwidth_info[src_ep][dest_ep], 3)
    
        channel = self.channel_map[src_ep][dest_ep]
        transferring_cost = cur_transffering_size / (channel.bandwidth * 1024 * 1024) / channel.num_of_workers
        transferring_cost = max(0, transferring_cost)

        for i in range(channel.num_of_workers):
            task_id = str(uuid.uuid4())
            tmp_task = TransferNode(
                task_id=task_id,
                file_size=cur_transffering_size / channel.num_of_workers,
                start_time=datetime.now(),
                end_time=datetime.now() + timedelta(seconds=transferring_cost)
            )
            
            # 使用新的数据结构存储任务
            channel.channels[i].append(tmp_task)
            # 在task_map中记录任务
            channel.task_map[task_id] = (i, tmp_task)


    def update_all_transfers(self, current_time, transferring_size, transferrring_num):
        completed_transfers = []
        for src_ep in self.channel_map:
            for dest_ep, channel in self.channel_map[src_ep].items():
                completed = channel.update_transfers(current_time , transferring_size, transferrring_num)
                if completed:
                    completed_transfers += completed
        return completed_transfers


    def submit_transfer_task(self, src_ep, dest_ep,file_size, current_time, transferring_size, transferrring_num):
        # 生成唯一的传输任务ID
        transfer_task_id = str(uuid.uuid4())
        
        
        # 获取传输通道并提交任务
        channel = self.channel_map[src_ep][dest_ep]
        expected_end_time = channel.push_transfer_task(
            transfer_task_id,
            file_size,
            current_time
        )
        if src_ep not in transferring_size:
            transferring_size[src_ep] = {}
            transferrring_num[src_ep] = {}
        if dest_ep not in transferring_size[src_ep]:
            transferring_size[src_ep][dest_ep] = 0
            transferrring_num[src_ep][dest_ep] = 0

        transferring_size[src_ep][dest_ep] += file_size
        transferrring_num[src_ep][dest_ep] += 1

        
        return transfer_task_id, expected_end_time

    
    def pop_task(self, src_ep, dest_ep, task_id):
        if src_ep not in self.channel_map or dest_ep not in self.channel_map[src_ep]:
            return None
            
        channel = self.channel_map[src_ep][dest_ep]
        return channel.pop_task(task_id)