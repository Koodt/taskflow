from taskflow import engines
from taskflow.patterns import linear_flow as lf
from taskflow import task
from taskflow import retry

import os


class FirstTask(task.Task):
    def __init__(self, name, show_name=True, inject=None):
        super(FirstTask, self).__init__(name, inject=inject)
        self._show_name = show_name

    def execute(self):
        # os.environ['ON_FAILURE'] = "2"
        if self._show_name:
            print("%s" % (self.name))


class RetryTask(retry.Times):
    def on_failure(self, history, *args, **kwargs):
        last_errors = history[-1][1]
        for task_name, ex_info in last_errors.items():
            # import pdb; pdb.set_trace()
            if len(history) <= 1:
                if ex_info is None or ex_info._exc_info is None:
                    return retry.RETRY
                excp = ex_info._exc_info[1]
                if isinstance(excp, Exception):
                    os.environ['ON_FAILURE'] = "1"
                    return retry.RETRY
        return retry.REVERT_ALL


class WithErrorTask(task.Task):
    def __init__(
        self,
        name,
        show_name=True,
        inject=None,
        fucking_manual_raise=None,
    ):
        super(WithErrorTask, self).__init__(name, inject=inject)
        self._show_name = show_name

    def execute(self):
        try:
            self.fucking_manual_raise = os.environ.get('ON_FAILURE')
        except BaseException:
            print("Not Found")
        if not self.fucking_manual_raise:
            print("KARAMBA %s" % self.fucking_manual_raise)
            raise Exception("Oops")
        if self._show_name:
            print("%s" % (self.name))


class SecondTask(task.Task):
    def __init__(
        self,
        name,
        show_name=True,
        inject=None,
        fucking_manual_raise=None
    ):
        super(SecondTask, self).__init__(name, inject=inject)
        self._show_name = show_name

    def execute(self):
        try:
            self.fucking_manual_raise = os.environ.get('ON_FAILURE')
        except BaseException:
            print("Not Found")
        print("KARAMBA %s" % self.fucking_manual_raise)
        if self._show_name:
            print("%s" % (self.name))


flow = lf.Flow("Retry-on-failure")
flow.add(FirstTask(name="FirstTask"))
retry_subflow = lf.Flow(
    "Retry-on-failure-subflow",
    retry=RetryTask()
)
retry_subflow.add(
    WithErrorTask(name="WithErrorTask")
)
flow.add(retry_subflow)
flow.add(SecondTask(name="SecondTask"))

e = engines.load(flow)
e.run()

print("-- Statistics gathered --")
print(e.statistics)
