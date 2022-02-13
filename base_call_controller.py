import argparse
from collections import deque
from collections import defaultdict
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

def update_deque(job_deque, basecall_dirs, fast5s_called_dict, job_number_dict):
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
                        "--compress_fastq -q 20000 --device auto").format(fast5_dir, input_list, save_dir)
            job_number_dict[sample_dir] += 1
            fast5s_called_dict[sample_id] = current_fast5_files

def main():
    args = parse_args()
    basecall_dirs = get_sample_basecall_dirs(args.experiment_dir, args.num_basecall_samples):
    for basecall_dir in basecall_dirs:
        os.mkdir(basecall_dir + '/fastq')
        os.mkdir(basecall_dir + '/fastq/tmp')

    fast5s_called_dict = defaultdict(set)
    job_number_dict = defaultdict(int)
    job_deque = deque()
    while len(job_deque) < args.num_basecall_samples:
        update_deque(job_deque, basecall_dirs, fast5s_called_dict, job_number_dict)
        time.sleep(200) #wait about 3 minutes for more fast5s to be written

    # now jobs are in queue
    while len(job_deque) > 0:
        while len(job_deque) > 0:
            while len(job_deque) > 0:
                job = job_deque.popleft()
                return_code = os.system(job)
            update_deque(job_deque, basecall_dirs, fast5s_called_dict, job_number_dict)

        time.sleep(300) #wait 5 minutes to see if any more fast5s are written
        update_deque(job_deque, basecall_dirs, fast5s_called_dict, job_number_dict)
