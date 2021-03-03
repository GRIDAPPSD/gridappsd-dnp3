import os
import sys
import time
import csv
import numpy as np
sys.path.append("../dnp3/service")

from dnp3.master import MyMaster, MyLogger, AppChannelListener, SOEHandler, MasterApplication
from dnp3.dnp3_to_cim import CIMMapping
from pydnp3 import opendnp3, openpal


def build_csv_writers(folder, filename, column_names):
    _file = os.path.join(folder, filename)
    if os.path.exists(_file):
        os.remove(_file)
    file_handle = open(_file, 'a')
    csv_writer = csv.writer(file_handle, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(column_names)
    # csv_writer.writerow(['epoch time'] + column_names)
    return file_handle, csv_writer

# def run_master(HOST="127.0.0.1",PORT=20000, DNP3_ADDR=10, convertion_type='Shark', object_name='632633'):
def run_master(device_ip_port_config_all, names):
    masters = []
    data_loc = '.'
    data_loc = '/media/sf_git_scada_read/GridAppsD_Usecase3/701_disag/701/test'
# dnp3_to_cim = CIMMapping(conversion_dict="conversion_dict_master.json", model_line_dict="model_line_dict.json")
    dnp3_to_cim = CIMMapping(conversion_dict=os.path.join(data_loc,"conversion_dict_master.json"), model_line_dict=os.path.join(data_loc,"model_line_dict_master.json"))
    for name in names:
        device_ip_port_dict = device_ip_port_config_all[name]
        HOST=device_ip_port_dict['ip']
        PORT=device_ip_port_dict['port']
        DNP3_ADDR= device_ip_port_dict['link_local_addr']
        LocalAddr= device_ip_port_dict['link_remote_addr']
        convertion_type=device_ip_port_dict['conversion_type']
        object_name=device_ip_port_dict['CIM object']

        application_1 = MyMaster(HOST=HOST,  # "127.0.0.1
                                LOCAL="0.0.0.0",
                                PORT=int(PORT),
                                DNP3_ADDR=int(DNP3_ADDR),
                                LocalAddr=int(LocalAddr),
                                log_handler=MyLogger(),
                                listener=AppChannelListener(),
                                soe_handler=SOEHandler(object_name, convertion_type, dnp3_to_cim),
                                master_application=MasterApplication())
        # application.channel.SetLogFilters(openpal.LogFilters(opendnp3.levels.ALL_COMMS))
        # print('Channel log filtering level is now: {0}'.format(opendnp3.levels.ALL_COMMS))
        masters.append(application_1)

    SLEEP_SECONDS = 1
    time.sleep(SLEEP_SECONDS)
    group_variation = opendnp3.GroupVariationID(32, 2)
    # time.sleep(SLEEP_SECONDS)
    # print('\nReading status 1')
    application_1.master.ScanRange(group_variation, 0, 12)
    # time.sleep(SLEEP_SECONDS)
    # print('\nReading status 2')
    application_1.master.ScanRange(opendnp3.GroupVariationID(32, 2), 0, 3, opendnp3.TaskConfig().Default())
    time.sleep(SLEEP_SECONDS)
    print('\nReading status 3')
    # application_1.slow_scan.Demand()

    # application_1.fast_scan_all.Demand()

    # for master in masters:
    #     master.fast_scan_all.Demand()
    msg_count=0
    rtu_7_csvfile, rtu_7_writer = build_csv_writers('.', 'rtu1.csv', ['count']+list(range(300)))
    while True:
        cim_full_msg = {'simulation_id': 1234, 'timestamp': 0, 'message':{'measurements':{}}}
        for master in masters:
            cim_msg = master.soe_handler.get_msg()
            dnp3_msg = master.soe_handler.get_dnp3_msg()
            max_index = max(dnp3_msg.keys())
            values  = [dnp3_msg[k] for k in range(max_index)]
            rtu_7_writer.writerow(np.insert(values,0, msg_count))
            rtu_7_csvfile.flush()
            # print(cim_msg)
            # message['message']['measurements']
            cim_full_msg['message']['measurements'].update(cim_msg)
            # self._gapps.send('/topic/goss.gridappsd.fim.input.'+str(1234), json.dumps(temp_cim_msg))
            msg_count+=1

        print(cim_full_msg)
        time.sleep(10)


    print('\nStopping')
    for master in masters:
        master.shutdown()
    # application_1.shutdown()
    exit()

    # When terminating, it is necessary to set these to None so that
    # it releases the shared pointer. Otherwise, python will not
    # terminate (and even worse, the normal Ctrl+C won't help).
    application_1.master.Disable()
    application_1 = None
    application_1.channel.Shutdown()
    application_1.channel = None
    application_1.manager.Shutdown()

if __name__ == "__main__":

    import argparse
    import json
    parser = argparse.ArgumentParser()
    parser.add_argument("names",  nargs='+', help="name of dnp3 outstation", type=str)
    # parser.add_argument("feeder_info",help='feeder info directory for y-matrix, etc.')
    args = parser.parse_args()
    print(args.names)
    # exit(0)
    with open("device_ip_port_config_all.json") as f:
        device_ip_port_config_all = json.load(f)

    device_ip_port_dict = device_ip_port_config_all[args.names[0]]
    print(device_ip_port_dict)
    run_master(device_ip_port_config_all, args.names)
    # run_master(device_ip_port_dict['ip'],
    #            device_ip_port_dict['port'],
    #            device_ip_port_dict['link_local_addr'],
    #            device_ip_port_dict['conversion_type'],
    #            '632633')