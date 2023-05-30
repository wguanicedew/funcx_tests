
from training.train1 import main1

def run_wrapper(base_path, data_path, func_str):
    import json
    import os
    import socket
    import sys

    current_dir = os.getcwd()
    os.chdir(base_path)

    os.environ['HARVESTER_WORKER_BASE_PATH'] = base_path
    os.environ['HARVESTER_DATA_PATH'] = data_path
    os.environ['PYTHONPATH'] = base_path + ":" + os.environ.get("PYTHONPATH", "")
    print("hostname: %s" % socket.gethostname())
    print("current directory: %s" % os.getcwd())
    print("PYTHONPATH: %s" % os.environ['PYTHONPATH'])
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

    os.chdir(current_dir)

    return ret_value


base_dir = '/data/idds/harvester_wdirs/Test_harvester_funcx/05/25/525/'
data_path = base_dir
# job_script = '{"func_name": "main1", "pre_script": "from training.train1 import main1", "kwargs": {"input_file": "%IN", "output_file": "output.tar.gz"}}'
job_script = '{"func_name": "main1", "pre_script": "from training.train1 import main1", "kwargs": {"input_file": "dataset_1_photons_1.hdf5", "output_file": "output.tar.gz", "example_run": true}}'

run_wrapper(base_dir, data_path, job_script)
