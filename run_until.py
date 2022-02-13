## Set script on timer once the run begins to run every ~30min 
### > while true; do python run_until.py --host "localhost" --port 9501 --target 60 ; sleep 1800; done


import argparse
# minknow_api.manager supplies "Manager" a wrapper around MinKNOW's Manager gRPC API with utilities for
# querying sequencing positions + offline basecalling tools.
from minknow_api.manager import Manager


def main():
    """Main entrypoint for run until"""
    parser = argparse.ArgumentParser(description="Stop sequencing once an estimated base troughput has been met.")
    parser.add_argument("--host", default="localhost", help="Specify which host to connect to.")
    parser.add_argument("--port", default=None, help="Specify which porer to connect to.")
    parser.add_argument("--target", default="60", help="Gigabase yield target to stop sequencing (in gigabases). [default 60]")
    parser.add_argument("--flowcell_positions", default=None, help="Comma-seperated list of flowcell positions to check [defaults to all currently running flowcells]")

    args = parser.parse_args()

    # Construct a manager using the host + port provided.
    manager = Manager(host=args.host, port=args.port, use_tls=False)
    target_yield = float(args.target) * 1000000000

    target_positions = None
    if args.flowcell_positions != None:
        target_positions = set(args.flowcell_positions.strip().split(','))


    # Find a list of currently available sequencing positions.
    positions = manager.flow_cell_positions()

    for pos in positions:
        if target_positions == None or pos.name in target_positions:
            connection = pos.connect()

            # check if flowcell is currently sequencing
            # 3 is enum code for PROCESSING
            if connection.acquisition.current_status().status != 3: continue

            current_yield = connection.acquisition.get_acquisition_info().yield_summary.estimated_selected_bases
            print("Flowcell at position %s currently sequencing, current yield: %.2f Gb" % (pos.name, current_yield / 1000000000))
            if current_yield > target_yield:
                print("Sequencing run in %s has sequenced an estimated %.2f Gb, stopping run." % (pos.name, current_yield / 1000000000))
                connection.protocol.stop_protocol()


if __name__ == "__main__":
    main()
