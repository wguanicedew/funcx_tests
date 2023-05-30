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

prun  --noBuild --site BNL_Funcx_Test --exec '{"func_name": "main1", "pre_script": "from training.train1 import main1", "kwargs": {"input_file": "%IN", "output_file": "output.tar.gz", "example_run": true}}' --inDS user.wguan:dataset_1_photons_1  --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat,output.tar.gz

prun  --noBuild --site BNL_Funcx_Test --exec '{"func_name": "main1", "pre_script": "from training.train1 import main1", "kwargs": {"input_file": "%IN", "output_file": "output.tar.gz"}}' --inDS user.wguan:dataset_1_photons_1  --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat,output.tar.gz
```
