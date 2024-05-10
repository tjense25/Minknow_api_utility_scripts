### > python run_until.py --host "localhost" --port 9501

import argparse
import time
import pandas as pd
# minknow_api.manager supplies "Manager" a wrapper around MinKNOW's Manager gRPC API with utilities for
# querying sequencing positions + offline basecalling tools.
from minknow_api.manager import Manager
from collections import defaultdict


def main():
    """Main entrypoint for run until"""
    parser = argparse.ArgumentParser(description="Stop sequencing once an estimated base troughput has been met.")
    parser.add_argument("--host", default="localhost", help="Specify which host to connect to.")
    parser.add_argument("--port", default=None, help="Specify which porer to connect to.")
    parser.add_argument("--target", default="140", help="Gigabase yield target to stop sequencing (in gigabases). [default 60]")
    parser.add_argument("--flowcell_positions", default=None, help="Comma-seperated list of flowcell positions to check [defaults to all currently running flowcells]")

    args = parser.parse_args()

    # Construct a manager using the host + port provided.
    print("connecting . . . ")
    manager = Manager(host=args.host, port=args.port)
    print("done connecting!!")

    print("assigning target yields:")
    target_yields = {}
    if (args.target.isalnum()):
        target_yields = defaultdict(lambda:float(args.target) * 1e9)
    else:
        sample_sheet = pd.read_table(args.target)
        target_yields = defaultdict(lambda:200 * 1e9)
        for i,row in sample_sheet.iterrows():
            target_yields[row.position_id] = row.target * 1e9

    target_positions = None
    if args.flowcell_positions != None:
        target_positions = set(args.flowcell_positions.strip().split(','))


    running_samples = set()
    finished_samples = set()
    #time.sleep(800)

    
    while True:
        # Find a list of currently available sequencing positions.
        positions = manager.flow_cell_positions()

        total_yield = 0
        for pos in positions:
            if pos.name in finished_samples: continue
            if target_positions == None or pos.name in target_positions:
                connection = pos.connect()

                # check if flowcell is currently sequencing
                # 3 is enum code for PROCESSING
                if connection.acquisition.current_status().status != 3: continue
                if pos.name in finished_samples: continue

                running_samples.add(pos.name)

                current_yield = connection.acquisition.get_acquisition_info().yield_summary.estimated_selected_bases
                current_pores = connection.acquisition.get_acquisition_info().bream_info.mux_scan_results[-1].counts['single_pore']

                total_yield += current_yield
                print("Flowcell at position %s currently sequencing, current yield: %.2f Gb, target yield: %.1f Gb, pores available: %d" % (pos.name, current_yield / 1e9, target_yields[pos.name]/1e9, current_pores))
                if current_yield > target_yields[pos.name] and current_pores > 1500:
                    print("Sequencing run in %s has sequenced an estimated %.2f Gb. Flowcell has %d pores left. Stopping run." % (pos.name, current_yield / 1e9, current_pores))
                    connection.protocol.stop_protocol()
                    finished_samples.add(pos.name)
                elif current_yield > target_yields[pos.name]:
                    print("Sequencing run in %s has hit target, with an estimated %.2f Gb, With only %d pores left, continuing sequencing to exhaustion." % (pos.name, current_yield / 1e9, current_pores ))
                    finished_samples.add(pos.name)

        if len(running_samples) == len(finished_samples):
            print("All sequencing jobs finished.")
            print("Estimated %.2f Gb sequenced" % (total_yield / 1000000000))
            print("Qutting now. Have a nice day :)")
            return

        print("{} samples currently sequencing.".format(len(running_samples) - len(finished_samples)))
        print("Estimated %.2f Gb sequenced." % (total_yield / 1000000000))
        print("{} runs  completed.".format(len(finished_samples)))
        print("Waiting 30 minutes to check progress again.")
        time.sleep(1800) # wait 30 minutes and then check yield again



if __name__ == "__main__":
    main()
