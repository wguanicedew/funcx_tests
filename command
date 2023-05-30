rucio upload --rse BNL-OSG2_SCRATCHDISK --scope user.wguan dataset_1_photons_1.hdf5
rucio add-dataset user.wguan:dataset_1_photons_1
rucio attach user.wguan:dataset_1_photons_1 user.wguan:dataset_1_photons_1.hdf5

rucio list-files user.wguan:dataset_1_photons_1

prun --exec "python purepython.py"  --inDS user.wguan:dataset_1_photons_1 --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat

prun --site BNL_Funcx_Test --exec "python purepython.py"  --inDS user.wguan:dataset_1_photons_1 --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat

prun  --noBuild --site BNL_Funcx_Test --exec "python purepython.py"  --inDS user.wguan:dataset_1_photons_1 --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat

#prun  --noBuild --site BNL_Funcx_Test --exec "from train import main;from argparse import ArgumentParser;parser = ArgumentParser(description='config for training');args = parser.parse_args(); args.input_file='${WORK_DIR}/dataset_1_photons_1.hdf5'; parser.output_path='${WORK_DIR}'; parser.config='${WORK_DIR}/config/config_GANv1.json'; parser.example_run=True; main(args, model='GANv1')"  --inDS user.wguan:dataset_1_photons_1 --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat

prun  --noBuild --site BNL_Funcx_Test --exec "from train import test; test(input_file='${WORK_DIR}/dataset_1_photons_1.hdf5', output_path='${WORK_DIR}', config='${WORK_DIR}/config/config_GANv1.json', example_run=True, model='GANv1')" --inDS user.wguan:dataset_1_photons_1 --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat

prun  --noBuild --site BNL_Funcx_Test --exec "from test_parsl_funcx import test_parsl; test_parsl() > out.dat" --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat

prun  --noBuild --site BNL_Funcx_Test --exec "print("test")" --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat

prun  --noBuild --site BNL_Funcx_Test --exec '{"func_name": "test_parsl", "pre_script": "from test_parsl_funcx1 import test_parsl"}' --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat

prun  --noBuild --site BNL_Funcx_Test --exec '{"func_name": "test_parsl", "pre_script": "from test_parsl_funcx2 import test_parsl", "kwargs": {"input_file": "%IN", "output_file": "output.tar.gz"}}' --inDS user.wguan:dataset_1_photons_1 --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat,output.tar.gz

prun  --noBuild --site BNL_Funcx_Test --exec '{"func_name": "main1", "pre_script": "from training.train1 import main1", "kwargs": {"input_file": "%IN", "output_file": "output.tar.gz"}}' --inDS user.wguan:dataset_1_photons_1  --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat,output.tar.gz

prun  --noBuild --site BNL_Funcx_Test --exec '{"func_name": "main1", "pre_script": "from training.train1 import main1", "kwargs": {"input_file": "%IN", "output_file": "output.tar.gz", "example_run": true}}' --inDS user.wguan:dataset_1_photons_1  --outDS user.wguan.`uuidgen` --nJobs 1 --output out.dat,output.tar.gz

