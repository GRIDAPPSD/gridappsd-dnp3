import os
import sys
import time
import platform

from pydnp3 import opendnp3, asiodnp3

sys.path.append("../dnp3/service")
from dnp3.dnp3_to_cim import CIMMapping

from outstation import OutstationApplication

# def run_master(HOST="127.0.0.1",PORT=20000, DNP3_ADDR=10, convertion_type='Shark', object_name='632633'):
def run_outstation(device_ip_port_config_all, names):
    masters = []
    data_loc = '.'
    if 'Darwin' != platform.system():
        data_loc = '/media/sf_git_scada_read/GridAppsD_Usecase3/701_disag/701/test'
    # dnp3_to_cim = CIMMapping(conversion_dict="conversion_dict_master.json", model_line_dict="model_line_dict.json")
    dnp3_to_cim = CIMMapping(conversion_dict=os.path.join(data_loc, "conversion_dict_master.json"),
                             model_line_dict=os.path.join(data_loc, "model_line_dict_master.json"))
    for name in names:
        device_ip_port_dict = device_ip_port_config_all[name]
        HOST=device_ip_port_dict['ip']
        PORT=device_ip_port_dict['port']
        DNP3_ADDR= device_ip_port_dict['link_local_addr']
        LocalAddr= device_ip_port_dict['link_remote_addr']
        convertion_type=device_ip_port_dict['conversion_type']
        object_name='632633'

        elements_to_device = {'632633': 'Shark'}
        application_1 = OutstationApplication(
                                LOCAL_IP="0.0.0.0",
                                PORT=int(PORT),
                                DNP3_ADDR=int(DNP3_ADDR),
                                LocalAddr=LocalAddr,
                                config=device_ip_port_dict)

        # application.channel.SetLogFilters(openpal.LogFilters(opendnp3.levels.ALL_COMMS))
        # print('Channel log filtering level is now: {0}'.format(opendnp3.levels.ALL_COMMS))
        masters.append(application_1)
    builder = asiodnp3.UpdateBuilder()
    while True:
        # cim_full_msg = {'simulation_id': 1234, 'timestamp': 0, 'messages':{}}
        for master in masters:
            print('Conversion type',master.config["conversion_type"])
            if 'RTU' in master.config["conversion_type"]:
                print('RTU',list(dnp3_to_cim.conversion_dict[master.config["conversion_type"]]['Analog input'].keys()))
                for index, value in dnp3_to_cim.conversion_dict[master.config["conversion_type"]]['Analog input'].items():
                # for index in range(100):
                    # master.apply_update(opendnp3.Analog(101.1223 + 2*float(index)), int(float(index)))
                    builder.Update(opendnp3.Analog(101.1223 + 2*float(index)), int(index))
            # else:
            #     for index, value in dnp3_to_cim.conversion_dict[master.config["conversion_type"]]['Analog input'].items():
            #         master.apply_update(opendnp3.Analog(1000.3 + 2*float(index)), int(float(index)))
            print("Updates Created")
            updates = builder.Build()
            OutstationApplication.get_outstation().Apply(updates)
        time.sleep(1)

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