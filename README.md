# funcx_tests


parsl example
-------------

```
cd normal

prun  --noBuild --site BNL_Funcx_Test --exec '{"func_name": "test_parsl", "pre_script": "from test_parsl_funcx1 import test_parsl"}' --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat

prun  --noBuild --site BNL_Funcx_Test --exec '{"func_name": "test_parsl", "pre_script": "from test_parsl_funcx2 import test_parsl", "kwargs": {"input_file": "%IN", "output_file": "output.tar.gz"}}' --inDS user.wguan:dataset_1_photons_1 --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat,output.tar.gz
```

ML example
----------
```
cd ml/funcXtraining/

# only run some example trainings

prun  --noBuild --site BNL_Funcx_Test --exec '{"func_name": "main1", "pre_script": "from training.train1 import main1", "kwargs": {"input_file": "%IN", "output_file": "output.tar.gz", "example_run": true}}' --inDS user.wguan:dataset_1_photons_1  --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat,output.tar.gz

# don't run this one currently
# this one will run a full training. It requires a big memory. However currently BNL_Funcx_Test attaches to an endpoint which doesn't have big resources. The worker will be killed. "2023-05-31 21:55:40,902 [KILL] -- Worker KILL message received!"

prun  --noBuild --site BNL_Funcx_Test --exec '{"func_name": "main1", "pre_script": "from training.train1 import main1", "kwargs": {"input_file": "%IN", "output_file": "output.tar.gz"}}' --inDS user.wguan:dataset_1_photons_1  --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat,output.tar.gz
```
