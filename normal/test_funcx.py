import time
# from funcx import FuncXClient

# fxc = FuncXClient()

# from funcx import FuncXExecutor

from globus_compute_sdk import Client
from globus_compute_sdk import Executor

fxc = Client()

# First, define the function ...
def add_func(a, b):
    return a + b

tutorial_endpoint_id = '4b116d3c-1703-4f8f-9f6f-39921e5864df' # Public tutorial endpoint

my_endpoint_id = "d614a625-a276-453b-afdc-42422e327573"

endpoint_id = my_endpoint_id

# ... then create the executor, ...
with Executor(endpoint_id=endpoint_id) as fxe:
    # ... then submit for execution, ...
    future = fxe.submit(add_func, 5, 10)

    print(future)
    # ... and finally, wait for the result
    print(future.result())



def hello_world():
    return "Hello World!"

func_uuid = fxc.register_function(hello_world)
print("hello_world uuid:")
print(func_uuid)


res = fxc.run(endpoint_id=endpoint_id, function_id=func_uuid)
print("run %s" % func_uuid)
print(res)

task = fxc.get_task(res)
while task['pending'] is True:
    time.sleep(0.1)
    task = fxc.get_task(res)
ret = fxc.get_result(res)
print(ret)

def funcx_sum(items):
    return sum(items)

sum_function = fxc.register_function(funcx_sum)
print("sum function uuid: %s" % sum_function)

items = [1, 2, 3, 4, 5]

res = fxc.run(items, endpoint_id=endpoint_id, function_id=sum_function)

task = fxc.get_task(res)
while task['pending'] is True:
    time.sleep(0.1)
    task = fxc.get_task(res)
ret = fxc.get_result(res)
print(ret)

def failing():
    raise Exception("deterministic failure")

failing_function = fxc.register_function(failing)

"""
print("failing function uuid: %s" % failing_function)
res = fxc.run(endpoint_id=endpoint_id, function_id=failing_function)

task = fxc.get_task(res)
while task['pending'] is True:
    time.sleep(0.1)
    task = fxc.get_task(res)
ret = fxc.get_result(res)
print(ret)
"""

endpoint_status = fxc.get_endpoint_status(endpoint_id)

print(endpoint_status)
print("Status: %s" % endpoint_status['status'])
print("Workers: %s" % endpoint_status['details']['total_workers'])
print("Tasks: %s" % endpoint_status['details']['outstanding_tasks'])
