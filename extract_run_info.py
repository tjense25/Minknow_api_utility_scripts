import sys
import json

seq_data = sys.argv[1]

seq_json = json.load(open(seq_data, 'r'))

#protocol info
sample_id = seq_json['protocol_run_info']['user_info']['sample_id']
position = seq_json['protocol_run_info']['device']['device_id']
flowcell = seq_json['protocol_run_info']['flow_cell']['flow_cell_id']

#acquisition info
## yield
throughput = seq_json['acquisitions'][-1]['acquisition_run_info']['yield_summary']['estimated_selected_bases']

# pores left
pore_count = seq_json['acquisitions'][-1]['acquisition_run_info']['bream_info']['mux_scan_results'][-1]['counts']['single_pore']

#n50
n50 = seq_json['acquisitions'][-1]['read_length_histogram'][3]['plot']['histogram_data'][0]['n50']

print('\t'.join(map(str,[sample_id, position, flowcell, int(throughput)/1e9, pore_count, int(n50)/1e3])))

