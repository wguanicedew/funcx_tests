import time
# from funcx import FuncXClient

# fxc = FuncXClient()

# from funcx import FuncXExecutor

from globus_compute_sdk import Client
from globus_compute_sdk import Executor
from globus_compute_sdk import errors as gc_errors

fxc = Client()

# First, define the function ...
def add_func(a, b):
    return a + b

tutorial_endpoint_id = '4b116d3c-1703-4f8f-9f6f-39921e5864df' # Public tutorial endpoint

my_endpoint_id = "d614a625-a276-453b-afdc-42422e327573"

endpoint_id = my_endpoint_id


def run_wrapper(base_path, data_path, func_str):
    import json
    import os
    import socket
    import sys
    os.environ['HARVESTER_WORKER_BASE_PATH'] = base_path
    os.environ['HARVESTER_DATA_PATH'] = data_path
    # os.environ['PYTHONPATH'] = base_path + ":" + os.environ.get("PYTHONPATH", "")
    # os.environ['PYTHONPATH'] = base_path
    print("hostname: %s" % socket.gethostname())
    print("current directory: %s" % os.getcwd())
    print("PYTHONPATH: %s" % os.environ.get('PYTHONPATH', None))
    print("execute programe: %s" % str(func_str))

    func_json = json.loads(func_str)
    func_name = func_json["func_name"]
    kwargs = func_json.get("kwargs", {})
    pre_script = func_json.get("pre_script", None)
    sys.path.append(base_path)

    if pre_script:
        exec(pre_script)

    f = locals()[func_name]
    ret_value = f(**kwargs)
    # ret = "hostname: %s\n" % socket.gethostname()
    # ret += "current directory: %s\n" % os.getcwd()
    # ret += "PYTHONPATH: %s\n" % os.environ['PYTHONPATH']
    # ret += "execute programe: %s\n" % str(func_str)
    # ret += "return value: %s" % ret_value
    print("return value: %s" % ret_value)
    return ret_value


func_uuid = fxc.register_function(run_wrapper)
print("run_wrapper uuid:")
print(func_uuid)


base_path = "/data/idds/harvester_wdirs/Test_harvester_funcx/03/18/318"
data_path = "/data/idds/rucio"
func_str = "from test_parsl_funcx1 import test_parsl; test_parsl()"
func_str = '{"func_name": "test_parsl", "pre_script": "from test_parsl_funcx1 import test_parsl", "kwargs": {"a": 1, "b": 2}}'
func_str = '{"func_name": "test_parsl", "pre_script": "from test_parsl_funcx1 import test_parsl"}'
# func_str = "print('test')"

print("run locally")
run_wrapper(base_path, data_path, func_str)


print("run in funcx")

res = fxc.run(base_path, data_path, func_str, endpoint_id=endpoint_id, function_id=func_uuid)
print("run %s" % func_uuid)
print(res)

try:
    task = fxc.get_task(res)
    while task['pending'] is True:
        time.sleep(0.1)
        task = fxc.get_task(res)
except Exception as ex:
    print("ex")
    print(ex)
except gc_errors.error_types.TaskExecutionFailed as ex:
    print("gc error")
    print(ex)

print("get_result:")
ret = fxc.get_result(res)
print(ret)

