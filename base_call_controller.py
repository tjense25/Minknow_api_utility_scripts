import argparse
from collections import defaultdict
import multiprocessing
import sys
import os
import glob
import time

def parse_args():
    """Build and execute a command line argument for basecalling

    Returns:
        Parsed arguments to be used when starting a protocol.
    """

    parser = argparse.ArgumentParser(
        description="""
        Guppy basecall a currently running sequencing experiment
        """
    )
    parser.add_argument(
        "--experiment_dir", 
        help="path to directory where sequencing experiment is running", 
        required=True,
    )
    parser.add_argument(
        "--num_basecall_samples",
        type=int,
        default=8,
        help="max number of samples to basecall, will not basecall samples that are already being basecalled through minknow",
    )
    args = parser.parse_args()
    return args

def get_sample_basecall_dirs(experiment_dir, num_basecall):
    basecall_samples = set()
    while True:
        samples = os.listdir(experiment_dir)
        fast5_dirs = [ glob.glob(experimnet_dir + '/' + sample_dir + '/**/fast5*') for sample in samples ]
        for sample,fast5_list in zip(samples, fast5_dirs):
            if len(fast5_list) == 0 or any([ os.path.basename(x) == "fast5_pass" for x in fast5_list]): continue 
            elif any([os.path.basename(x) == "fast5" for x in fast5_list]): 
                basecall_dirs.add(os.path.dirname(fast5_list[0]))

            if len(basecall_samples) >= num_basecall: return basecall_samples
        time.sleep(120) #wait 2 minutes for data generation to begin and then check fast5 files again

def run_job(job_command):
    process_id = multiprocessing.Process()._identity[1]
    #ensure the parallel jobs are using different gpu devices
    if process_id == 1: 
        job_command += " --device CUDA:0"
    elif process_id == 2:
        job_command += " --device CUDA:1"
    os.system(job_command)

def update_job_list(basecall_dirs, fast5s_called_dict, job_number_dict):
    job_list = []
    for sample_dir in basecall_dirs:
        fast5_dir = sample_dir + '/fast5'
        current_fast5_files =  set(os.listdir(sample_dir))
        new_fast5s = current_fast5_files.difference(fast5s_called_dict[sample_dir])
        if len(new_fast5s) > 0:
            input_list = sample_dir + '/fastq/tmp/fast5_list_%d.txt' % job_number_dict[sample_dir]
            save_dir = sample_dir + '/fastq/' + 'guppy_job_%d' % job_number_dict[sample_dir]
            print('\n'.join(list(new_fast5s), file = fast5_file))
            command = ("guppy_basecaller --disable_pings" 
                        "--input_path {} --input_file_list {}"
                        "--save_path {} --min_qscore 7 "
                        "-c dna_r9.4.1_450bps_hac_prom.cfg " 
                        "--compress_fastq -q 50000 ").format(fast5_dir, input_list, save_dir)
            job_list.append(command)
            job_number_dict[sample_dir] += 1
            fast5s_called_dict[sample_id] = current_fast5_files
    return job_list

def initialize_fast5_dir(basecall_dirs, fast5s_called_dict, job_number_dict):
    for sample_dir in basecall_dirs:
        if not os.path.exists(sample_dir + '/fastq/tmp'):
            return

        fast5_file_lists = os.listdir(sample_dir + '/fastq/tmp'):
        for fast5_file_list in fast5_file_lists:
            with open(fast5_file_list, 'r') as f_in:
                for line in f_in:
                    fast5s_called_dict[sample_dir].add(line.strip())
            job_num_dict[sample_dir] += 1


def main():
    args = parse_args()
    basecall_dirs = get_sample_basecall_dirs(args.experiment_dir, args.num_basecall_samples)
    fast5s_called_dict = defaultdict(set)
    job_number_dict = defaultdict(int)
    fast5s_called_dict = initialize_fast5_dir(basecall_dirs, fast5s_called_dict, job_number_dict)
    for basecall_dir in basecall_dirs:
        if not os.path.exists(basecall_dir + '/fastq'):
            os.mkdir(basecall_dir + '/fastq')
        if not os.path.exists(basecall_dir + '/fastq/tmp'):
            os.mkdir(basecall_dir + '/fastq/tmp')

    job_list = []
    while len(job_list) < args.num_basecall_samples:
        job_list = update_job_list(basecall_dirs, fast5s_called_dict, job_number_dict)
        time.sleep(200) #wait about 3 minutes for more fast5s to be written

    # now jobs are in queue
    pool = multiprocessing.Pool(processes = 2)
    while len(job_list) > 0:
        while len(job_list) > 0:
            pool.map(run_job, job_list)
            job_list = update_job_list(basecall_dirs, fast5s_called_dict, job_number_dict)

        time.sleep(600) #wait 10 minutes to see if any more fast5s are written
        job_list = update_job_list(basecall_dirs, fast5s_called_dict, job_number_dict)
    print("completed all basecalling job. Quitting now".
    print("Have a wonderful day UwU")
