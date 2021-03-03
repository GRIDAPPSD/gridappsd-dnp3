import os
import sys
import time
sys.path.append("../dnp3/service")

from dnp3.master import MyMaster, MyLogger, AppChannelListener, SOEHandler, MasterApplication, SOEHandlerSimple
from dnp3.dnp3_to_cim import CIMMapping
from pydnp3 import opendnp3, openpal
import yaml
from dnp3.points import PointValue
# from dnp3.points import (
#     PointArray, PointDefinitions, PointDefinition, DNP3Exception, POINT_TYPE_ANALOG_INPUT, POINT_TYPE_BINARY_INPUT
# )

master = None

def collection_callback(result=None):
    """
    :type result: opendnp3.CommandPointResult
    """
    print("Header: {0} | Index:  {1} | State:  {2} | Status: {3}".format(
        result.headerIndex,
        result.index,
        opendnp3.CommandPointStateToString(result.state),
        opendnp3.CommandStatusToString(result.status)
    ))
    # print(result)


def command_callback(result=None):
    """
    :type result: opendnp3.ICommandTaskResult
    """
    print("Received command result with summary: {}".format(opendnp3.TaskCompletionToString(result.summary)))
    result.ForeachItem(collection_callback)

from gridappsd import GridAPPSD, DifferenceBuilder, utils
from time import sleep

def on_message(simulation_id,message):
    print("Message received:", simulation_id['message-id'])
    print(message)
    json_msg = yaml.safe_load(str(message))
    # self.master.apply_update(opendnp3.Binary(0), 12)

    # self.master.send_direct_operate_command(
    #     opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON),
    #     5,
    #     command_callback)

    if type(json_msg) != dict:
        raise ValueError(
            ' is not a json formatted string.'
            + '\njson_msg = {0}'.format(json_msg))

    # fncs_input_message = {"{}".format(simulation_id): {}}

    global master
    cap_point = PointValue(command_type=None, function_code=None, value=1, point_def=0, index=0, op_type=None)
    cap_point.measurement_id = "_5955BE75-5EE5-477A-936F-65EDE5E3B831"
    # "_5955BE75-5EE5-477A-936F-65EDE5E3B831"
    # master = masters[0]

    # {"command": "update", "input": {"simulation_id": "1312790133", "message": {"timestamp": 1614730782, "difference_mrid": "e8287b64-6802-49b5-a6f0-12d8048fd98e", "reverse_differences": [{"object": "_5955BE75-5EE5-477A-936F-65EDE5E3B831", "attribute": "ShuntCompensator.sections", "value": 1}, {"object": "_C1706031-2C1C-464C-8376-6A51FA70B470", "attribute": "ShuntCompensator.sections", "value": 1}, {"object": "_245E3924-8292-46D5-A11E-C80F7D6EE253", "attribute": "ShuntCompensator.sections", "value": 1}, {"object": "_D081C22C-D840-4303-8C95-C9151610C9A6", "attribute": "TapChanger.step", "value": 0.0}], "forward_differences": [{"object": "_5955BE75-5EE5-477A-936F-65EDE5E3B831", "attribute": "ShuntCompensator.sections", "value": 0}, {"object": "_C1706031-2C1C-464C-8376-6A51FA70B470", "attribute": "ShuntCompensator.sections", "value": 0}, {"object": "_245E3924-8292-46D5-A11E-C80F7D6EE253", "attribute": "ShuntCompensator.sections", "value": 0}, {"object": "_D081C22C-D840-4303-8C95-C9151610C9A6", "attribute": "TapChanger.step", "value": 0.0}]}}}

    # print(json_msg)
    # received message {'command': 'update', 'input': {'simulation_id': '1764973334', 'message': {'timestamp': 1597447649, 'difference_mrid': '5ba5daf7-bf8b-4458-bc23-40ea3fb8078f', 'reverse_differences': [{'object': '_A9DE8829-58CB-4750-B2A2-672846A89753', 'attribute': 'ShuntCompensator.sections', 'value': 1}, {'object': '_9D725810-BFD6-44C6-961A-2BC027F6FC95', 'attribute': 'ShuntCompensator.sections', 'value': 1}], 'forward_differences': [{'object': '_A9DE8829-58CB-4750-B2A2-672846A89753', 'attribute': 'ShuntCompensator.sections', 'value': 0}, {'object': '_9D725810-BFD6-44C6-961A-2BC027F6FC95', 'attribute': 'ShuntCompensator.sections', 'value': 0}]}}}
    if "input" in json_msg.keys():
        print("input Jeff")
        control_values = json_msg["input"]["message"]["forward_differences"]
        print(control_values)

        for command in control_values:
            print("command", command)
            # master = self.master_dict[command["object"]]
            # print(master)
            point = cap_point
            # Capbank
            if command.get("object") == point.measurement_id and point.value != command.get("value"):
                open_cmd = command.get("value") == 0
                if open_cmd:
                    # Open
                    master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON),
                                                            0,  # PULSE/LATCH_ON to index 0 for open
                                                            command_callback)
                    master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON),
                                                            1,  # PULSE/LATCH_ON to index 1 for open
                                                            command_callback)
                    master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON),
                                                            2,  # PULSE/LATCH_ON to index 2 for open
                                                            command_callback)
                    cap_point.value = 0
                else:
                    # Will need 5 minutes after open operation for this capbank
                    # Close
                    # master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON),
                    #                                                     0, # PULSE/LATCH_ON to index 0 for close
                    #                                                     command_callback)
                    master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_OFF),
                                                            0,  # PULSE/LATCH_ON to index 0 for close
                                                            command_callback)
                    master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_OFF),
                                                            1,  # PULSE/LATCH_ON to index 0 for close
                                                            command_callback)
                    master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_OFF),
                                                            2,  # PULSE/LATCH_ON to index 0 for close
                                                            command_callback)
                    cap_point.value = 1
            elif command.get("object") == point.measurement_id and point.value == command.get("value"):
                print("Cap check", command.get("object"), command.get("value"))

            pv_point_tmp1 = PointValue(command_type=None, function_code=None, value=0, point_def=0, index=1, op_type=None)
            pv_point_tmp1.measurement_id = "_5D0562C7-FE25-4FEE-851E-8ADCD69CED3B"
            pv_point_tmp2 = PointValue(command_type=None, function_code=None, value=0, point_def=0, index=2, op_type=None)
            pv_point_tmp2.measurement_id = "_5D0562C7-FE25-4FEE-851E-8ADCD69CED3B"
            pv_points = [pv_point_tmp1,pv_point_tmp2]
            # PV points
            for point in pv_points:
                pass
                # master.send_direct_operate_command(opendnp3.AnalogOutputInt32(7),
                #                                                  1,
                #                                                  command_callback)

                # master.send_direct_operate_command(opendnp3.AnalogOutputInt32(14),
                #                                                  2,
                #                                                  command_callback)

    # master.send_select_and_operate_command(opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON),
    #                                        1,
    #                                        command_callback)

# def run_master(HOST="127.0.0.1",PORT=20000, DNP3_ADDR=10, convertion_type='Shark', object_name='632633'):
def run_master(device_ip_port_config_all, names):
    masters = []
    dnp3_to_cim = CIMMapping(conversion_dict="conversion_dict_master.json", model_line_dict="model_line_dict.json")
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
                                # soe_handler=SOEHandler(object_name, convertion_type, dnp3_to_cim),
                                soe_handler=SOEHandlerSimple(),
                                master_application=MasterApplication())
        masters.append(application_1)

    SLEEP_SECONDS = 1
    time.sleep(SLEEP_SECONDS)

    global master
    master = masters[0]

    simulation_id = str(1234)
    gapps = GridAPPSD(simulation_id, address=utils.get_gridappsd_address(),
                      username=utils.get_gridappsd_user(), password=utils.get_gridappsd_pass())

    gapps.subscribe('/topic/goss.gridappsd.fim.output.' + simulation_id, on_message)
    while True:
        sleep(0.01)

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