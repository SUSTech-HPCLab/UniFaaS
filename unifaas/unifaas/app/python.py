import logging

import tblib.pickling_support

tblib.pickling_support.install()

from unifaas.app.app import AppBase
from unifaas.app.errors import wrap_error
from unifaas.dataflow.dflow import DataFlowKernelLoader


logger = logging.getLogger("unifaas")


def timeout(f, seconds):
    def wrapper(*args, **kwargs):
        import threading
        import ctypes
        import unifaas.app.errors

        def inject_exception(thread):
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_long(thread), ctypes.py_object(unifaas.app.errors.AppTimeout)
            )

        thread = threading.current_thread().ident
        timer = threading.Timer(seconds, inject_exception, args=[thread])
        timer.start()
        result = f(*args, **kwargs)
        timer.cancel()
        return result

    return wrapper


class PythonApp(AppBase):
    """Extends AppBase to cover the Python App."""

    def __init__(
        self,
        func,
        data_flow_kernel=None,
        cache=False,
        executors="all",
        ignore_for_cache=[],
        join=False,
        never_change=False,
        compress_option=(None,None,None,None),
    ):
        super().__init__(
            wrap_error(func),
            data_flow_kernel=data_flow_kernel,
            executors=executors,
            cache=cache,
            ignore_for_cache=ignore_for_cache,
        )
        self.join = join
        self.never_change = never_change
        self.compress_option = compress_option

    def __call__(self, *args, **kwargs):
        """This is where the call to a python app is handled.

        Args:
             - Arbitrary
        Kwargs:
             - Arbitrary

        Returns:
                   App_fut

        """
        invocation_kwargs = {}
        invocation_kwargs.update(self.kwargs)
        invocation_kwargs.update(kwargs)

        if self.data_flow_kernel is None:
            dfk = DataFlowKernelLoader.dfk()
        else:
            dfk = self.data_flow_kernel

        walltime = invocation_kwargs.get("walltime")
        if walltime is not None:
            func = timeout(self.func, walltime)
        else:
            func = self.func

        app_fut = dfk.submit(
            func,
            app_args=args,
            executors=self.executors,
            cache=self.cache,
            ignore_for_cache=self.ignore_for_cache,
            app_kwargs=invocation_kwargs,
            join=self.join,
            never_change=self.never_change,
            compress_option=self.compress_option
        )

        return app_fut
