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


def hello_world():
    import subprocess

    cmd = 'echo {"43451671": {"StatusCode": 0, "PandaID": 43451671, "prodSourceLabel": "test", "swRelease": "NULL", "homepackage": "NULL", "transformation": "http://pandaserver-doma.cern.ch:25080/trf/user/runGen-00-00-02", "jobName": "user.wguan.117eea76-ba54-42fa-8fd6-7ed6d1a2b705/.43451671", "jobDefinitionID": 0, "cloud": "Rubin", "inFiles": "", "dispatchDblock": "", "dispatchDBlockToken": "", "dispatchDBlockTokenForOut": "NULL,NULL", "outFiles": "user.wguan.117eea76-ba54-42fa-8fd6-7ed6d1a2b705.log.151988.000003.log.tgz,user.wguan.151988._000003.myout.txt", "destinationDblock": "user.wguan.117eea76-ba54-42fa-8fd6-7ed6d1a2b705.log/,user.wguan.117eea76-ba54-42fa-8fd6-7ed6d1a2b705_myout.txt/", "destinationDBlockToken": "NULL,NULL", "prodDBlocks": "", "prodDBlockToken": "", "realDatasets": "user.wguan.117eea76-ba54-42fa-8fd6-7ed6d1a2b705.log/,user.wguan.117eea76-ba54-42fa-8fd6-7ed6d1a2b705_myout.txt/", "realDatasetsIn": "", "fileDestinationSE": "NULL,NULL", "logFile": "user.wguan.117eea76-ba54-42fa-8fd6-7ed6d1a2b705.log.151988.000003.log.tgz", "logGUID": "d17c54f5-32d5-4810-8b56-f5cd2b46122b", "jobPars": "-j \\"\\" --sourceURL https://pandaserver-doma.cern.ch:25443 -r . -l ${LIB} -o \\"{\'myout.txt\': \'user.wguan.151988._000003.myout.txt\'}\\" -p \\"python%20test_parsl_funcx.py%20%3E%20myout.txt\\"", "attemptNr": 1, "GUID": "", "checksum": "", "fsize": "", "scopeIn": "", "scopeOut": "user.wguan", "scopeLog": "user.wguan", "ddmEndPointIn": "", "ddmEndPointOut": ",", "destinationSE": "NULL", "prodUserID": "Wen Guan", "maxCpuCount": 0, "minRamCount": 1800, "maxDiskCount": 300, "cmtConfig": "NULL", "processingType": "panda-client-1.5.22-jedi-run", "transferType": "NULL", "sourceSite": "NULL", "currentPriority": 1000, "taskID": 151988, "coreCount": 1, "jobsetID": 151988, "reqID": 151988, "nucleus": "NULL", "maxWalltime": 0, "ioIntensity": null, "ioIntensityUnit": null, "nSent": 3, "inFilePaths": ""}} > pandaJobData.out;'
    cmd += "export PANDA_JSID=harvester-Test_harvester_funcx;"
    cmd += "export HARVESTER_ID=Test_harvester_funcx; "
    cmd += "export HARVESTER_WORKER_ID=211;"
    cmd += "export OIDC_AUTH_DIR=$(pwd);"
    cmd += "export OIDC_AUTH_TOKEN=oidc_token;"

    cmd += "export VO_ATLAS_SW_DIR=/tmp/; "

    cmd += "echo eyJraWQiOiJyc2ExIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI0Y2ZmNDQxOC0xYmUzLTRkNTYtYjI3NC0wMmE2NjM2OTg0NWUiLCJhdWQiOiJodHRwczpcL1wvcGFuZGFzZXJ2ZXItZG9tYS5jZXJuLmNoOjI1NDQzIiwiaXNzIjoiaHR0cHM6XC9cL3BhbmRhLWlhbS1kb21hLmNlcm4uY2giLCJleHAiOjE2ODE3MzgwNjIsImlhdCI6MTY4MTM5MjQ2MiwianRpIjoiNjVkZmNjNGQtOWQ2Ny00MDg1LWExMjktMTY4NTFhODA5MzM4In0.CGDl_1lk48bam2AdD26cnoIhVOa1_sicqK0YM1kRT-y_pORla1BtDKMDjhvwk-qqrpuUf-B0jgotwx9NvKeLwxgFLMZ7Fa_NS70Vh41q8eUwWnvn1TVNkvb7vpWFBeKtBrwKa7q5BcEv6xUXscFEqochtBkhLW7PtuHQMi3xyUQ > $OIDC_AUTH_DIR/$OIDC_AUTH_TOKEN;"
    cmd += "export OIDC_AUTH_ORIGIN=panda_dev.pilot;"
    cmd += "wget http://pandaserver-doma.cern.ch:25080/cache/schedconfig/FUNCX_TEST.all.json -O queuedata.json;"
    cmd += "wget https://raw.githubusercontent.com/PanDAWMS/pilot-wrapper/master/runpilot2-wrapper.sh;"
    cmd += "chmod +x runpilot2-wrapper.sh; bash runpilot2-wrapper.sh --localpy --piloturl http://cern.ch/atlas-panda-pilot/pilot3-3.6.0.48.tar.gz  -t -s FUNCX_TEST -r FUNCX_TEST -q FUNCX_TEST -j managed -i PR -d --harvester-submit-mode PUSH -w generic --pilot-user ATLAS --url https://pandaserver-doma.cern.ch -d --harvester-submit-mode PUSH --queuedata-url http://pandaserver-doma.cern.ch:25080/cache/schedconfig/FUNCX_TEST.all.json --storagedata-url https://datalake-cric.cern.ch/api/atlas/ddmendpoint/query/?json --noproxyverification"

    # export PANDA_JSID=harvester-Test_harvester_funcx; export HARVESTER_ID=Test_harvester_funcx; export HARVESTER_WORKER_ID=211; export PANDA_AUTH_TOKEN=eyJraWQiOiJyc2ExIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI0Y2ZmNDQxOC0xYmUzLTRkNTYtYjI3NC0wMmE2NjM2OTg0NWUiLCJhdWQiOiJodHRwczpcL1wvcGFuZGFzZXJ2ZXItZG9tYS5jZXJuLmNoOjI1NDQzIiwiaXNzIjoiaHR0cHM6XC9cL3BhbmRhLWlhbS1kb21hLmNlcm4uY2giLCJleHAiOjE2ODE3MzgwNjIsImlhdCI6MTY4MTM5MjQ2MiwianRpIjoiNjVkZmNjNGQtOWQ2Ny00MDg1LWExMjktMTY4NTFhODA5MzM4In0.CGDl_1lk48bam2AdD26cnoIhVOa1_sicqK0YM1kRT-y_pORla1BtDKMDjhvwk-qqrpuUf-B0jgotwx9NvKeLwxgFLMZ7Fa_NS70Vh41q8eUwWnvn1TVNkvb7vpWFBeKtBrwKa7q5BcEv6xUXscFEqochtBkhLW7PtuHQMi3xyUQ; export PANDA_AUTH_ORIGIN=panda_dev.pilot; wget https://raw.githubusercontent.com/PanDAWMS/pilot-wrapper/master/runpilot2-wrapper.sh; chmod +x runpilot2-wrapper.sh; bash runpilot2-wrapper.sh -s FUNCX_TEST -r FUNCX_TEST -q None -j managed -i PR -d --harvester-submit-mode PUSH -w generic --pilot-user ATLAS --url https://pandaserver.cern.ch -d --harvester-submit-mode PUSH'
    p = subprocess.Popen(cmd, shell=True, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    stdout, stderr = p.communicate()
    return p.returncode, cmd + str(stdout), stderr


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

