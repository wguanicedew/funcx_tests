#!/usr/bin/env python

import argparse
import os
import shlex
import tarfile
import urllib.request as urllib


job_data = {'5847310458': {'StatusCode': 0, 'PandaID': 5847310458, 'prodSourceLabel': 'user', 'swRelease': 'NULL', 'homepackage': 'NULL', 'transformation': 'http://pandaserver.cern.ch:25085/trf/user/runGen-00-00-02', 'jobName': 'user.wguan.9a9d9873-9b20-4685-8e7d-d43f9728b221/.5847310458', 'jobDefinitionID': 88447, 'cloud': 'US', 'inFiles': '', 'dispatchDblock': '', 'dispatchDBlockToken': '', 'dispatchDBlockTokenForOut': 'NULL,NULL', 'outFiles': 'user.wguan.33455917._000001.out.dat,user.wguan.9a9d9873-9b20-4685-8e7d-d43f9728b221.log.33455917.000001.log.tgz', 'destinationDblock': 'user.wguan.9a9d9873-9b20-4685-8e7d-d43f9728b221_out.33455917_sub3899267,user.wguan.9a9d9873-9b20-4685-8e7d-d43f9728b221.33455917_sub3899268', 'destinationDBlockToken': 'NULL,NULL', 'prodDBlocks': '', 'prodDBlockToken': '', 'realDatasets': 'user.wguan.9a9d9873-9b20-4685-8e7d-d43f9728b221_out.dat/,user.wguan.9a9d9873-9b20-4685-8e7d-d43f9728b221.log/', 'realDatasetsIn': '', 'fileDestinationSE': 'BNL_Funcx_Test/SCORE,BNL_Funcx_Test/SCORE', 'logFile': 'user.wguan.9a9d9873-9b20-4685-8e7d-d43f9728b221.log.33455917.000001.log.tgz', 'logGUID': 'be482151-400c-4eb3-a413-f464d36f1edf', 'jobPars': '-j "" --sourceURL https://aipanda048.cern.ch -r . -a jobO.13c83f47-3fd7-40ee-832e-60499e965676.tar.gz -o "{\'out.dat\': \'user.wguan.33455917._000001.out.dat\'}" -p "from%20test_parsl_funcx%20import%20test_parsl%3B%20test_parsl%28%29%20%3E%20out.dat"', 'attemptNr': 1, 'GUID': '', 'checksum': '', 'fsize': '', 'scopeIn': '', 'scopeOut': 'user.wguan', 'scopeLog': 'user.wguan', 'ddmEndPointIn': '', 'ddmEndPointOut': 'BNL-OSG2_SCRATCHDISK,BNL-OSG2_SCRATCHDISK', 'destinationSE': 'NULL', 'prodUserID': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=wguan/CN=667815/CN=Wen Guan/CN=2075422820', 'maxCpuCount': 345600, 'minRamCount': 1800, 'maxDiskCount': 300, 'cmtConfig': '@centos8', 'processingType': 'panda-client-1.5.50-jedi-run', 'transferType': 'direct', 'sourceSite': 'NULL', 'currentPriority': 999, 'taskID': 33455917, 'coreCount': 1, 'jobsetID': 88446, 'reqID': 88446, 'nucleus': 'NULL', 'maxWalltime': 345600, 'ioIntensity': None, 'ioIntensityUnit': None, 'nSent': 0, 'inFilePaths': ''}}



def get_panda_argparser():
    parser = argparse.ArgumentParser(description='PanDA argparser')
    parser.add_argument('-j', type=str, required=False, default='', help='j')
    parser.add_argument('--sourceURL', type=str, required=False, default='', help='source url')
    parser.add_argument('-r', type=str, required=False, default='', help='directory')
    parser.add_argument('-l', '--lib',  required=False, action='store_true', default=False, help='l')
    parser.add_argument('-o', '--output', type=str, required=False, default='', help='output')
    parser.add_argument('-p', '--program', type=str, required=False, default='', help='parameter')
    parser.add_argument('-a', '--archive', type=str, required=False, default='', help='source file')
    return parser


def get_job_args(job_data):
    job_id = list(job_data.keys())[0]
    job_info = job_data[job_id]
    job_pars = job_info['jobPars']
    job_args = shlex.split(job_pars)
    parser = get_panda_argparser()
    args, unknown = parser.parse_known_args(job_args)
    return args


def download_source_codes(base_dir, source_url, source_file):
    print("source_url: %s, source_file: %s" % (source_url, source_file))
    archive_basename = os.path.basename(source_file)
    if not os.path.exists(base_dir):
        os.makedirs(base_dir, exist_ok=True)
    full_output_filename = os.path.join(base_dir, archive_basename)

    os.environ["PANDACACHE_URL"] = source_url + "/server/panda"
    # os.environ["PANDA_URL_SSL"] = os.environ["PANDACACHE_URL"]
    # print(os.environ["PANDA_URL_SSL"])
    from pandaclient import Client
    status, output = Client.getFile(archive_basename, output_path=full_output_filename)
    print("Download archive file from pandacache status: %s, output: %s" % (status, output))
    if status != 0:
        raise RuntimeError("Failed to download archive file from pandacache")
    with tarfile.open(full_output_filename, 'r:gz') as f:
        f.extractall(base_dir)
    print("Extract %s to %s" % (full_output_filename, base_dir))


if __name__ == "__main__":
    job_args = get_job_args(job_data)
    print(job_args)
    # download_source_codes("./test/", job_args.sourceURL, job_args.archive)
    job_program = job_args.program
    print(job_program)

    p = urllib.unquote(job_program)
    print(p)
