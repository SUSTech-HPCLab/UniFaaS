import psutil


class SystemInfoUtil:

    @staticmethod
    def get_current_cpu_info(interval=0.1):
        cpu_info = {}
        cpu_percent = psutil.cpu_times_percent(interval=interval)
        cpu_info['cores'] = psutil.cpu_count()
        cpu_info['user_cpu'] = cpu_percent.user
        cpu_info['system_cpu'] = cpu_percent.system
        cpu_info['idle_cpu'] = cpu_percent.idle
        cpu_info['cpu_freq'] = psutil.cpu_freq().max
        return cpu_info

    @staticmethod
    def get_current_mem_info():
        mem_info = {}
        ps_mem = psutil.virtual_memory()
        mem_info['total_mem'] = ps_mem.total / 10 ** 6
        mem_info['avail_mem'] = ps_mem.available / 10 ** 6
        mem_info['used_mem'] = ps_mem.used / 10 ** 6
        return mem_info
