from parsl.utils import RepresentationMixin

from funcx_endpoint.executors import HighThroughputExecutor

_DEFAULT_EXECUTORS = [HighThroughputExecutor()]

    
class Config(RepresentationMixin):
    """Specification of FuncX configuration options.

    Parameters
    ----------

    executors : list of Executors
        A list of executors which serve as the backend for function execution.
        As of 0.2.2, this list should contain only one executor.
        Default: [HighThroughtputExecutor()]

    funcx_service_address: str
        URL address string of the funcX service to which the Endpoint should connect.
        Default: 'https://api2.funcx.org/v2'

    heartbeat_period: int (seconds)
        The interval at which heartbeat messages are sent from the endpoint to the
        funcx-web-service
        Default: 30s

    heartbeat_threshold: int (seconds)
        Seconds since the last hearbeat message from the funcx-web-service after which
        the connection is assumed to be disconnected.
        Default: 120s

    stdout : str
        Path where the endpoint's stdout should be written
        Default: ./interchange.stdout

    stderr : str
        Path where the endpoint's stderr should be written
        Default: ./interchange.stderr

    detach_endpoint : Bool
        Should the endpoint deamon be run as a detached process? This is good for
        a real edge node, but an anti-pattern for kubernetes pods
        Default: True

    log_dir : str
        Optional path string to the top-level directory where logs should be written to.
        Default: None

    globus_ep_id : str
        Globus endpoint ID. Default: None

    globus_polling_interval : float
        The interval (in seconds) to poll data transfer status. Default: 10, minimum 1

    local_data_path : str
        The local path to store the data. Default: None
    """

    def __init__(
        self,
        # Execution backed
        executors: list = _DEFAULT_EXECUTORS,
        # Connection info
        funcx_service_address="https://api2.funcx.org/v2",
        # Tuning info
        heartbeat_period=30,
        heartbeat_threshold=120,
        detach_endpoint=True,
        # Logging info
        log_dir=None,
        stdout="./endpoint.log",
        stderr="./endpoint.log",
        # Globus transfer info
        transfer_method="globus",
        globus_ep_id="xxx-xxx-xxx-xxx",
        host_ip=None,
        host_username=None,
        check_rsync_auth=False,
        rsync_password_file=None,
        globus_polling_interval=10,
        local_data_path="~/.funcx/data_transfer",  # the file path on the endpoint to store data
        transfer_command_server_config=None,
        force_start_config=None,
    ):

        # Execution backends
        self.executors = executors  # List of executors

        # Connection info
        self.funcx_service_address = funcx_service_address

        # Tuning info
        self.heartbeat_period = heartbeat_period
        self.heartbeat_threshold = heartbeat_threshold
        self.detach_endpoint = detach_endpoint

        self.transfer_method = transfer_method
        ava_transfer_methods = ["sftp", "globus", "rsync"]
        if self.transfer_method not in ava_transfer_methods:
            raise ValueError(
                f"Invalid transfer method {self.transfer_method}. "
                f"Available transfer methods are {ava_transfer_methods}"
            )

        if local_data_path is None:
            raise ValueError(
                "local_data_path is required for data transfer"
            )

        if self.transfer_method == "globus" and globus_ep_id is None:
            raise ValueError(
                "Globus endpoint ID is required for 'globus' transfer method"
            )
        
        if self.transfer_method == "rsync" :
            if host_ip is None:
                raise ValueError(
                    "host_ip is required for 'rsync' transfer method"
                )
            if host_username is None:
                raise ValueError(
                    "host_username is required for 'rsync' transfer method"
                )
        
        if self.transfer_method == "sftp":
            if host_ip is None:
                raise ValueError(
                    "host_ip is required for 'sftp' transfer method"
                )
            if host_username is None:
                raise ValueError(
                    "host_username is required for 'sftp' transfer method"
                )
            if transfer_command_server_config is None:
                raise ValueError(
                    "transfer_command_server_config is required for establishing a 'sftp' server"
                )
        


        # Logging info
        self.log_dir = log_dir
        self.stdout = stdout
        self.stderr = stderr

    
        # Globus transfer info
        self.globus_ep_id = globus_ep_id
        self.globus_polling_interval = globus_polling_interval
        if local_data_path is not None and not local_data_path.endswith("/"):
            local_data_path = local_data_path+"/"
        self.local_data_path = local_data_path


        # Since interchange is lanuched as a subprocess, we need to pass a string-format
    
        self.host_ip = host_ip
        self.host_username = host_username
        self.rsync_ip = host_ip
        self.rsync_username = host_username
        self.check_rsync_auth = check_rsync_auth
        if rsync_password_file is not None:
            import os
            rsync_password_file = os.path.abspath(rsync_password_file)
        self.rsync_password_file = rsync_password_file
        self.transfer_command_server_config = transfer_command_server_config
        self.force_start_config = force_start_config
