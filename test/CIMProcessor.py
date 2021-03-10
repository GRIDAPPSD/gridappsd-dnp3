
from dnp3.master import MyMaster, MyLogger, AppChannelListener, SOEHandler, MasterApplication, SOEHandlerSimple
from dnp3.dnp3_to_cim import CIMMapping
from pydnp3 import opendnp3, openpal
import yaml
from dnp3.points import PointValue

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

class CIMProcessor(object):

    def __init__(self, point_definitions,master):
        self.point_definitions = point_definitions # TODO
        self._master = master

    def process(self, message):
        master = self._master
        # print("Message received:", simulation_id['message-id'])
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
# "capacitors":[
# {"name":"701-104cf","mRID":"_5955BE75-5EE5-477A-936F-65EDE5E3B831","CN1":"cf_701_311","phases":"ABC","kvar_A":400.0,"kvar_B":400.0,"kvar_C":400.0,"nominalVoltage":12000.0,"nomU":12000.0,"phaseConnection":"Y","grounded":true,"enabled":true,"mode":"voltage","targetValue":7080.5,"targetDeadband":57.8,"aVRDelay":0.0,"monitoredName":"701_70799_mc","monitoredClass":"ACLineSegment","monitoredBus":"fu_701_9492","monitoredPhase":"B"},
# {"name":"701-275cw","mRID":"_C1706031-2C1C-464C-8376-6A51FA70B470","CN1":"fu_701_20042","phases":"ABC","kvar_A":100.0,"kvar_B":100.0,"kvar_C":100.0,"nominalVoltage":12000.0,"nomU":12000.0,"phaseConnection":"Y","grounded":true,"enabled":true,"mode":"voltage","targetValue":7080.5,"targetDeadband":57.8,"aVRDelay":0.0,"monitoredName":"701_167489_mc","monitoredClass":"ACLineSegment","monitoredBus":"cw_701_1710","monitoredPhase":"B"},
# {"name":"701-319cw","mRID":"_245E3924-8292-46D5-A11E-C80F7D6EE253","CN1":"cw_701_347","phases":"ABC","kvar_A":400.0,"kvar_B":400.0,"kvar_C":400.0,"nominalVoltage":12000.0,"nomU":12000.0,"phaseConnection":"Y","grounded":true,"enabled":true,"mode":"voltage","targetValue":7080.5,"targetDeadband":57.8,"aVRDelay":0.0,"monitoredName":"701_57838_mc","monitoredClass":"ACLineSegment","monitoredBus":"fu_701_9026","monitoredPhase":"B"}
# ],
# "regulators":[
# {"bankName":"s1","size":"1","bankPhases":"ABC","tankName":[""],"endNumber":[2],"endPhase":["ABC"],"rtcName":["s1"],"mRID":["_D081C22C-D840-4303-8C95-C9151610C9A6"],"monitoredPhase":["A"],"TapChanger.tculControlMode":["volt"],"highStep":[16],"lowStep":[-16],"neutralStep":[0],"normalStep":[0],"TapChanger.controlEnabled":[true],"lineDropCompensation":[true],"ltcFlag":[true],"RegulatingControl.enabled":[true],"RegulatingControl.discrete":[true],"RegulatingControl.mode":["voltage"],"step":[-8],"targetValue":[123.0000],"targetDeadband":[2.0000],"limitVoltage":[0.0000],"stepVoltageIncrement":[0.6250],"neutralU":[6927.6000],"initialDelay":[30.0000],"subsequentDelay":[2.0000],"lineDropR":[7.0000],"lineDropX":[0.0000],"reverseLineDropR":[0.0000],"reverseLineDropX":[0.0000],"ctRating":[300.0000],"ctRatio":[1500.0000],"ptRatio":[57.7300]}
# ],

        cap_point1 = PointValue(command_type=None, function_code=None, value=1, point_def=0, index=0, op_type=None)
        cap_point1.measurement_id = "_5955BE75-5EE5-477A-936F-65EDE5E3B831"
        cap_point2 = PointValue(command_type=None, function_code=None, value=1, point_def=0, index=0, op_type=None)
        cap_point2.measurement_id = "_C1706031-2C1C-464C-8376-6A51FA70B470"
        cap_point3 = PointValue(command_type=None, function_code=None, value=1, point_def=0, index=0, op_type=None)
        cap_point3.measurement_id = "_245E3924-8292-46D5-A11E-C80F7D6EE253"
        cap_list = [cap_point1,cap_point2,cap_point3]
        reg_point1 = PointValue(command_type=None, function_code=None, value=100, point_def=0, index=0, op_type=None)
        reg_point1.measurement_id = "_D081C22C-D840-4303-8C95-C9151610C9A6"
        reg_list = [reg_point1]
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
                for point in reg_list:
                    if command.get("object") == point.measurement_id and point.value != command.get("value"):
                        point.value = command.get("value")
                        print("Send reg value "+ str(command.get("value")))
                        temp_value = int(command.get("value"))
                        master.send_direct_operate_command(opendnp3.AnalogOutputInt32(temp_value),
                                                                     0,
                                                                     command_callback) 

                for point in cap_list:
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
                            point.value = 0
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
                            point.value = 1
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