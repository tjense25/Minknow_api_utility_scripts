### > python run_until.py --host "localhost" --port 9501

import argparse
import time
import statistics
import plotext as plt
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
    parser.add_argument("--target", default="210", help="Gigabase yield target to stop sequencing (in gigabases). [default 210]")

    args = parser.parse_args()

    # Construct a manager using the host + port provided.
    print("connecting . . . ")
    manager = Manager(host=args.host, port=args.port)
    print("done connecting!!")

    target_yield = float(args.target) * 1e9

    start_time = time.time()
    yields={}
    sequencing_times = []
    total_yields = []
    plt.plot_size(90,25)
    plt.theme('dark')
    while True:
        fc_yields = []
        seq_time = time.time() - start_time
        # Find a list of currently available sequencing positions.
        positions = manager.flow_cell_positions()

        total_yield = 0
        for pos in positions:
                connection = pos.connect()
                # check if flowcell is currently sequencing
                # 3 is enum code for PROCESSING
                if connection.acquisition.current_status().status != 3: continue
                current_yield = connection.acquisition.get_acquisition_info().yield_summary.estimated_selected_bases
                yields[pos.name] = current_yield
                #print("Flowcell at position %s currently sequencing, current yield: %.2f Gb" % (pos.name, current_yield / 1e9))
        fc_yields = list(map(lambda x: x[1], yields.items()))
        plot_yields = [ x /1e9 for x in fc_yields]
        total_yield = sum(fc_yields)
        if len(total_yields) > 0:
            max_fc = max(yields.items(), key = lambda x: x[1])
            min_fc = min(yields.items(), key = lambda x: x[1])
            median_fc = statistics.median(fc_yields)
            seq_rate = ((total_yield - total_yields[-1])/1e9) / ((seq_time - sequencing_times[-1])/60)
            time_left = (target_yield - total_yield) / seq_rate
            ## Histogram 
            plt.ylim(0,5)
            plt.hist(plot_yields,50)
            plt.title("Flowcell Throughput (Gb)")
            plt.show()
            plt.clear_data()
            print("Min throughput: %.2f (%s) \t\t Median throughput: %.2f \t\t Max throughput: %.2f (%s)" % (min_fc[1] / 1e9, min_fc[0], median_fc / 1e9, max_fc[1] / 1e9, max_fc[0]))

            ## Throughput line plot
            plt.ylim(0,250)
            plt.xlim(0,180)
            plt.plot([x/60 for x in sequencing_times + [seq_time]], [y /1e9 for y in total_yields + [total_yield]])
            plt.show()
            plt.clear_data()
            print("Sequencing Time: %.2f minutes\t\t Total Sequenced: %.2f Gb \t\t Gb left til Target: %.2f" % (seq_time / 60, total_yield / 1e9, (target_yield - total_yield)/1e9))
            print("Sequencing Rate: %.3f Gb/min \t\t Estimated time til target: %.2f minutes" % (seq_rate, time_left / 1e9))
        sequencing_times.append(seq_time)
        total_yields.append(total_yield)
        if total_yield >= target_yield:
            print("Sequenced a total of %.2f Gb, Stopping protocols on all positions" % total_yield / 1e9)
            positions = manager.flow_cell_positions()
            for pos in positions:
                connection = pos.connect()
                connection.protocol.stop_protocol()
            return

        print("Waiting 1 minutes to check progress again.")
        time.sleep(20) # wait 1 minute and then check yield again

    print("Have a nice day :)")


if __name__ == "__main__":
    main()
