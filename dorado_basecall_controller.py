import argparse
from collections import defaultdict
import multiprocessing
import sys
import os
import glob
import time

"""
python dorado_basecall_controller.py --pod5_list /samples_to_basecall.pod5_list.txt --num_gpus 4
"""

def parse_args():
    """Build and execute a command line argument for basecalling

    Returns:
        Parsed arguments to be used when starting a protocol.
    """

    parser = argparse.ArgumentParser(
        description="""
        DORADO basecall a list of pod5 directories
        """
    )
    parser.add_argument(
        "--pod5_list",
        help="list of pod5 directories to basecall",
        required=False
    )
    parser.add_argument(
        "--num_gpus",
        type=int,
        default=4,
        help="Number of CUDA gpu decies on machine. (Prom_beta has 2, p48 has 4) [Defaults to 2] "
    )
    parser.add_argument(
        "--flowcell_pore",
        type=str,
        help="flowcell R9 or R10 to be used to select which dorado model to run",
        default="r10"
    )
    args = parser.parse_args()
    return args


def run_job(job_command):
    process_id = multiprocessing.Process()._identity[0]
    #ensure the parallel jobs are using different gpu devices
    dorado_end_idx = job_command.find('>')
    #job_command = job_command[:dorado_end_idx] + " --device cuda:{} ".format(process_id - 1)  + job_command[dorado_end_idx:]
    job_command = job_command[:dorado_end_idx] + " --device cuda:all " + job_command[dorado_end_idx:]
    print(job_command)
    os.system(job_command)

def initialize_jobs_from_list(pod5_list_file, flowcell_pore):
    job_list = []
    pod5_dirs = [ line.strip() for line in open(pod5_list_file, 'r') ] 
    #model = {"r10":"/data/tanner_scripts/DORADO/dorado-0.3.4-linux-x64/bin/dna_r10.4.1_e8.2_400bps_sup@v4.2.0",
    #        "r9":"/data/tanner_scripts/DORADO/dorado-0.3.4-linux-x64/bin/dna_r9.4.1_e8_sup@v3.3"}
    #model = {"r10":"/data/tanner_scripts/dorado-0.5.2-linux-x64/bin/dna_r10.4.1_e8.2_400bps_sup@v4.3.0_5mCG_5hmCG@v1"}
    model = {"r10":"sup,5mCG_5hmCG"}
    for pod5_dir in pod5_dirs:
        out_bam = os.path.dirname(pod5_dir) + "/" + os.path.basename(pod5_dir) + ".bam"
        out_message = os.path.dirname(pod5_dir) + "/" + os.path.basename(pod5_dir) + ".BASECALLING_COMPLETE"
        checkpoint_bam = out_bam.strip('.bam') + ".checkpoint.bam"
        command = ("/data/tanner_scripts/dorado-0.6.1-linux-x64/bin/dorado basecaller -r " +
            " {} "
            " {} "
            " > {} "
            " && touch {} ").format(model[flowcell_pore],pod5_dir, out_bam, out_message)
        if os.path.isfile(out_message): continue
        if os.path.isfile(out_bam) and os.path.getsize(out_bam) > 0:
            os.rename(out_bam, checkpoint_bam) 
        if os.path.isfile(checkpoint_bam) and os.path.getsize(checkpoint_bam) > 0:
            command = ("/data/tanner_scripts/dorado-0.6.1-linux-x64/bin/dorado basecaller -r " +
                " {} "
                " {} "
                " --resume-from {} "
                " > {} "
                " && touch {} "
                " && rm {} ").format(model[flowcell_pore],pod5_dir,checkpoint_bam,out_bam,out_message,checkpoint_bam)
        job_list.append(command)
    return job_list
    
def main():
    args = parse_args()
    if args.flowcell_pore != "r10" and args.flowcell_pore != "r9": 
        print("flowcell_pore must be either r10 or r9. quiting.")
        return()
    job_list = initialize_jobs_from_list(args.pod5_list, args.flowcell_pore)
    pool = multiprocessing.Pool(processes = args.num_gpus)
    pool.map(run_job, job_list)
    print("completed all basecalling jobs. Quitting now.")
    print("Have a wonderful day")

if __name__ == "__main__":
    main()
