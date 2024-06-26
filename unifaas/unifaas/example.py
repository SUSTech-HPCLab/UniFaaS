import unifaas
from unifaas.config import Config
from unifaas.executors import FuncXExecutor
from funcx.sdk.file import GlobusFile
from funcx.sdk.file import RsyncFile
from funcx.sdk.file import RemoteFile
from unifaas.app.app import python_app


fx_config = Config(executors=[ 
            FuncXExecutor(
                label=f"EVA",
                endpoint="047c5616-975a-4c88-b195-0de65e886a07",
                funcx_service_address="http://10.16.52.172:5000/v2",
                poll_interval=5,
                batch_interval=3,
                batch_size=100,
                dev_mode=False,
            ),
            #  FuncXExecutor(
            #     label=f"lab02",
            #     endpoint="bc8f920f-3563-44ab-9f28-1f3070cca06c",
            #     funcx_service_address="http://10.16.52.172:5000/v2",
            #     poll_interval=5,
            #     batch_interval=3,
            #     batch_size=100,
            #     dev_mode=False,
            # ),
         ],
        enable_schedule=True,
        enable_execution_recorder=True,
        enable_duplicate=False,
        scheduling_strategy="DATA",
        transfer_type="sftp",
        password_file="/home/eric/.unifaas/password.csv",
        bandwidth_info={
            "EVA" : {"EVA": 1000, "taiyi":1 , "lab03":111, "cse_cluster":80, "data_pool":111, "qiming":1},
            "lab03" : {"EVA": 70, "taiyi":1, "lab03":1000,  "cse_cluster":110, "data_pool":110, "qiming":1},
            "cse_cluster" : {"EVA": 68.2, "taiyi":1 , "lab03":112,  "cse_cluster":1000, "data_pool":112, "qiming":1},
            "data_pool" : {"EVA": 70, "taiyi":1 , "lab03":1000,  "cse_cluster":110, "data_pool":1000, "qiming":1},
            "taiyi" : {"EVA": 1.5, "taiyi":1000 , "lab03":1.5,  "cse_cluster":1.5, "data_pool":1.5, "qiming":1.5},
            "qiming" : {"EVA": 1.5, "taiyi":3 , "lab03":1.5,  "cse_cluster":1.5, "data_pool":1.5, "qiming":1000},
        },
        workflow_name="22pmheft_big",
    )
unifaas.load(fx_config)


@python_app
def hello():
    return "Hello World!"

@python_app(compressor="gzip")
def wf_entry():
    from funcx.sdk.file import RsyncFile
    res_file = RsyncFile.remote_init(file_path="/home/eric/research/EVA_dir/CAS_1.csv", remote_ip="10.16.57.154", remote_username="eric", file_size=897427)
    return res_file

@python_app
def file_task2(f1):
    from funcx.sdk.file import RsyncFile
    if isinstance(f1, RsyncFile):
        fp = f1.get_remote_file_path()
        with open(fp, "r") as f:
            return len(f.readlines())
    return None
    

print(hello().result())

#print(file_task2(wf_entry()).result())
