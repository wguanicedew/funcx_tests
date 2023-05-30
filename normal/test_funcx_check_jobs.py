import time
# from funcx import FuncXClient

# fxc = FuncXClient()

# from funcx import FuncXExecutor

from globus_compute_sdk import Client
from globus_compute_sdk import Executor
from globus_compute_sdk import errors as gc_errors
fxc = Client()


tutorial_endpoint_id = '4b116d3c-1703-4f8f-9f6f-39921e5864df' # Public tutorial endpoint

my_endpoint_id = "d614a625-a276-453b-afdc-42422e327573"

endpoint_id = my_endpoint_id

res = "dfd37e7f-d726-47ac-aa70-0154b508cdc4"

try:
    task = fxc.get_task(res)
    print(task)
except gc_errors.error_types.TaskExecutionFailed as ex:
    print(ex)

while task['pending'] is True:
    time.sleep(0.1)
    task = fxc.get_task(res)
ret = fxc.get_result(res)
print(ret)

