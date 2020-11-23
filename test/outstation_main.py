import os
import sys
import time

from pydnp3 import opendnp3

sys.path.append("../dnp3/service")
from dnp3.dnp3_to_cim import CIMMapping

from outstation import OutstationApplication

# def run_master(HOST="127.0.0.1",PORT=20000, DNP3_ADDR=10, convertion_type='Shark', object_name='632633'):
def run_outstation(device_ip_port_config_all, names):
    masters = []
    dnp3_to_cim = CIMMapping(conversion_dict="conversion_dict.json", model_line_dict="model_line_dict.json")
    for name in names:
        device_ip_port_dict = device_ip_port_config_all[name]
        HOST=device_ip_port_dict['ip']
        PORT=device_ip_port_dict['port']
        DNP3_ADDR= device_ip_port_dict['link_local_addr']
        convertion_type=device_ip_port_dict['conversion_type']
        object_name='632633'

        elements_to_device = {'632633': 'Shark'}
        application_1 = OutstationApplication(
                                LOCAL_IP="0.0.0.0",
                                PORT=int(PORT),
                                DNP3_ADDR=int(DNP3_ADDR), config=device_ip_port_dict)

        # application.channel.SetLogFilters(openpal.LogFilters(opendnp3.levels.ALL_COMMS))
        # print('Channel log filtering level is now: {0}'.format(opendnp3.levels.ALL_COMMS))
        masters.append(application_1)

    while True:
        # cim_full_msg = {'simulation_id': 1234, 'timestamp': 0, 'messages':{}}
        for master in masters:
            master.config["conversion_type"]
            for index, value in dnp3_to_cim.conversion_dict[master.config["conversion_type"]]['Analog input'].items():
                print(index)
                int(float(index))
                master.apply_update(opendnp3.Analog(1000.3 + float(index)), int(float(index)))
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
    run_outstation(device_ip_port_config_all, args.names)
    # run_master(device_ip_port_dict['ip'],
    #            device_ip_port_dict['port'],
    #            device_ip_port_dict['link_local_addr'],
    #            device_ip_port_dict['conversion_type'],
    #            '632633')