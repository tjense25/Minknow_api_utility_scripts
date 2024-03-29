"""
Example script to start a protocol

Example usage might be:

python ./python/minknow_api/examples/start_protocol.py \
    --host localhost                                        # specify local host
    --sample_sheet sample_sheet.tsv \                       # tab seperated table with sample_id\tflowcell_position\texperiment_group
    --kit SQK-LSK110 \                                      # Specify which kit is being run
    --experiment_duration 72 \                              # Set the run time of the experiment
    --fast5_reads_per_file 10000 \
    --fastq_reads_per_file 10000 \
    --min_qscore 7 \
    --num_basecall_samples 16 
"""

import argparse
import logging
import pandas as pd
import sys

# minknow_api.manager supplies "Manager" a wrapper around MinKNOW's Manager gRPC API with utilities
# for querying sequencing positions + offline basecalling tools.
from enum import Enum
from typing import Optional, NamedTuple, Sequence
from minknow_api.manager import Manager, FlowCellPosition

# We need `find_protocol` to search for the required protocol given a kit + product code.
from minknow_api.protocol_pb2 import ProtocolRunUserInfo
from minknow_api.tools import protocols


def parse_args():
    """Build and execute a command line argument for starting a protocol.

    Returns:
        Parsed arguments to be used when starting a protocol.
    """

    parser = argparse.ArgumentParser(
        description="""
        Run a sequencing protocol in a running MinKNOW instance.
        """
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="IP address of the machine running MinKNOW (defaults to localhost)",
    )
    parser.add_argument(
        "--port",
        default=None,
        help="Port to connect to on host (defaults to standard MinKNOW port based on tls setting)",
    )
    parser.add_argument(
        "--use_tls", 
        help="Use a secure tls connection", 
        default=False, 
        action="store_true"
    )
    parser.add_argument(
        "--kit",
        default="SQK-LSK110",
        help="Sequencing kit used with the flow-cell, eg: SQK-LSK108",
    )
    # SAMPLE SHEET
    parser.add_argument(
        "--sample_sheet",
        help="Filename of CSV sample sheet. ",
    )
    parser.add_argument(
        "--fast5_reads_per_file",
        type=int,
        default=10000,
        help="set the number of reads combined into one Fast5 file.",
    )
    parser.add_argument(
        "--fastq_reads_per_file",
        type=int,
        default=10000,
        help="set the number of reads combined into one fastq file"
    )
    # Experiment
    parser.add_argument(
        "--experiment_duration",
        type=float,
        default=48,
        help="time spent sequencing (in hours)",
    )
    parser.add_argument(
        "--mux_scan_period",
        type=float,
        default=1.5,
        help="number of hours before a mux scan takes place, enables active-channel-selection, "
        "ignored for Flongle flow-cells",
    )
    parser.add_argument(
        "--num_basecall_samples",
        type=int,
        default=12,
        help="number of samples to basecall on tower [default 16]",
    )
    parser.add_argument(
        "--min_qscore",
        type=int,
        default=7,
        help="qscore threshold above which read 'passes' basecalling"
    )
    args = parser.parse_args()

    return args

ParsedSampleSheetEntry = NamedTuple(
    "ParsedSampleSheetEntry",
    [
        ("position_id", Optional[str]),
        ("sample_id", Optional[str]),
        ("experiment_id", Optional[str]),
    ]
)

class ExperimentSpec(object):
    def __init__(self, entry: ParsedSampleSheetEntry):
        self.entry = entry
        self.position = None
        self.basecalling = False
        self.protocol_id = ""


ExperimentSpecs = Sequence[ExperimentSpec]

# Add sample sheet entry info to experiment_specs
def add_sample_sheet_entries(experiment_specs : ExperimentSpecs, args):
    if args.sample_sheet:
        sample_sheet = pd.read_table(args.sample_sheet)
        for i,row in sample_sheet.iterrows():
            # Add the entry to the specs
            experiment_specs.append(
                ExperimentSpec(
                    entry=ParsedSampleSheetEntry(
                        position_id=row.position_id,
                        sample_id=row.sample_id,
                        experiment_id=row.experiment_id,
                    )
                )
            )

def add_position_to_specs(experiment_specs: ExperimentSpecs, position):
    # Look up by position_id
    matches = [
        spec for spec in experiment_specs if spec.entry.position_id == position.name
    ]

    if not matches:
        return
    if len(matches) > 1:
        print("Trying to start multiple experiments on the same flow cell")
        sys.exit(1)

    #print(matches)
    matches[0].position = position



# Add position info to the experiment_specs
def add_position_info(experiment_specs, manager):
    positions = manager.flow_cell_positions()
    for position in positions:
        add_position_to_specs(experiment_specs, position)


def add_basecalling_info(experiment_specs: ExperimentSpecs, args):
    for i,spec in enumerate(experiment_specs):
        if i < args.num_basecall_samples:
            spec.basecalling = True
        else: return

# Determine which protocol to run for each experiment, and add its ID to experiment_specs
def add_protocol_ids(experiment_specs, args):
    print(experiment_specs)
    for spec in experiment_specs:
        print(spec)
        # Connect to the sequencing position:
        print(spec.position)
        position_connection = spec.position.connect()

        # Check if a flowcell is available for sequencing
        flow_cell_info = position_connection.device.get_flow_cell_info()
        if not flow_cell_info.has_flow_cell:
            print("No flow cell present in position {}".format(spec.position))
            sys.exit(1)

        print(flow_cell_info)
        product_code = flow_cell_info.product_code
        if not product_code:
            product_code = flow_cell_info.user_specified_product_code

        # Find the protocol identifier for the required protocol:
        protocol_info = protocols.find_protocol(
            position_connection,
            product_code=product_code,
            kit=args.kit,
            basecalling=spec.basecalling
        )

        # Store the identifier for later:
        spec.protocol_id = protocol_info.identifier

def main():
    """Entrypoint to start protocol example"""
    # Parse arguments to be passed to started protocols:
    args = parse_args()

    # Construct a manager using the host + port provided:
    manager = Manager(host=args.host, port=args.port) 

    experiment_specs = []
    add_sample_sheet_entries(experiment_specs, args)
    add_position_info(experiment_specs, manager)
    add_basecalling_info(experiment_specs, args)
    add_protocol_ids(experiment_specs, args)

    # Now start the protocol(s):
    print("Starting protocol on %s positions" % len(experiment_specs))
    for spec in experiment_specs:
        position_connection = spec.position.connect()

        protocol_arguments = [
           "--experiment_time={}".format(args.experiment_duration),
           "--start_bias_voltage=-165",
           "--fast5=on",
           "--fast5_data",
           "raw",
           "fastq",
           "vbz_compress",
           "--min_read_length=200",
           "--generate_bulk_file=off",
           "--active_channel_selection=on",
           "--pore_reserve=off",
           "--fast5_reads_per_file={}".format(args.fast5_reads_per_file),
           "--mux_scan_period={}".format(args.mux_scan_period),
           "--guppy_filename=dna_r9.4.1_450bps_hac_prom.cfg",
           "--bam=off",
        ]
        if spec.basecalling:
            protocol_arguments.extend([
                "--base_calling=on",
                "--fastq=on",
                "--fastq_data",
                "compress",
                "--fastq_reads_per_file={}".format(args.fastq_reads_per_file),
                "--read_filtering",
                "min_qscore={}".format(args.min_qscore)
            ])
        else:
            protocol_arguments.extend([
                "--base_calling=off",
                "--fastq=off",
                ])

        user_info = ProtocolRunUserInfo()
        user_info.sample_id.value = spec.entry.sample_id
        user_info.protocol_group_id.value = spec.entry.experiment_id
        position_connection.protocol.start_protocol(
                identifier=spec.protocol_id,
                args=protocol_arguments,
                user_info=user_info
        )

        flow_cell_info = position_connection.device.get_flow_cell_info()

        print("Started protocol:")
        print("    position={}".format(spec.position.name))
        print("    flow_cell_id={}".format(flow_cell_info.flow_cell_id))


if __name__ == "__main__":
    main()
