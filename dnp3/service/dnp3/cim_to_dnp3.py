import json
import yaml
import sys
import datetime
import random
import uuid

from pydnp3 import opendnp3
from typing import List, Dict, Union, Any
from dnp3.outstation import DNP3Outstation
from dnp3.points import (
    PointArray, PointDefinitions, PointDefinition, DNP3Exception, POINT_TYPE_ANALOG_INPUT, POINT_TYPE_BINARY_INPUT
)

from dnp3.master import command_callback, restart_callback

out_json = list()

'''Dictionary for mapping the attribute values of control pointd for Capacitor, Regulator and Switches, Mesurements'''

attribute_map = {
    "capacitors": {
        "attribute": ["RegulatingControl.mode", "RegulatingControl.targetDeadband", "RegulatingControl.targetValue",
                      "ShuntCompensator.aVRDelay", "ShuntCompensator.sections"]}
    ,
    "switches": {
        "attribute": "Switch.open"
    }
    ,

    "regulators": {
        "attribute": ["RegulatingControl.targetDeadband", "RegulatingControl.targetValue", "TapChanger.initialDelay",
                      "TapChanger.lineDropCompensation", "TapChanger.step", "TapChanger.lineDropR",
                      "TapChanger.lineDropX"]},

    "measurements": {
        "attribute": ["measurement_mrid", "type", "magnitude", "angle", "value"]}
        # ["Discrete", "Analog" ,"Measurement.PowerSystemResource", "Measurement.Terminal",
        #  "Measurement.phases","Measurement.measurementType"]  ## TODO check against points file

}
class DNP3Mapping():
    """ This creates dnp3 input and output points for incoming CIM messages  and model dictionary file respectively."""

    def __init__(self, map_file):
        self.c_ao = 0
        self.c_do = 0
        self.c_ai = 0
        self.c_di = 0
        self.measurements = dict()
        self.out_json = list()
        self.file_dict = map_file
        self.processor_point_def = PointDefinitions()
        self.outstation = DNP3Outstation('',0,'')
        self.master = None


    def on_message(self, simulation_id, message):
        """ This method handles incoming messages on the fncs_output_topic for the simulation_id.
        Parameters
        ----------
        headers: dict
            A dictionary of headers that could be used to determine topic of origin and
            other attributes.
        message: object

        """

        try:
            message_str = 'received message ' + str(message)
            print(message_str)
            json_msg = yaml.safe_load(str(message))
            # self.master.apply_update(opendnp3.Binary(0), 12)

            self.master.send_direct_operate_command(
                opendnp3.ControlRelayOutputBlock(opendnp3.ControlCode.LATCH_ON),
                5,
                command_callback)


            if type(json_msg) != dict:
                raise ValueError(
                    ' is not a json formatted string.'
                    + '\njson_msg = {0}'.format(json_msg))

            # fncs_input_message = {"{}".format(simulation_id): {}}
            # measurement_values = json_msg["message"]["measurements"]
            measurement_values = []

            print(json_msg)
            # received message {'command': 'update', 'input': {'simulation_id': '1764973334', 'message': {'timestamp': 1597447649, 'difference_mrid': '5ba5daf7-bf8b-4458-bc23-40ea3fb8078f', 'reverse_differences': [{'object': '_A9DE8829-58CB-4750-B2A2-672846A89753', 'attribute': 'ShuntCompensator.sections', 'value': 1}, {'object': '_9D725810-BFD6-44C6-961A-2BC027F6FC95', 'attribute': 'ShuntCompensator.sections', 'value': 1}], 'forward_differences': [{'object': '_A9DE8829-58CB-4750-B2A2-672846A89753', 'attribute': 'ShuntCompensator.sections', 'value': 0}, {'object': '_9D725810-BFD6-44C6-961A-2BC027F6FC95', 'attribute': 'ShuntCompensator.sections', 'value': 0}]}}}
            control_values = json_msg["input"]["message"]["forward_differences"]
            print(control_values)
            # exit(0)

            ## TODO get command
            for command in control_values:
                print("command", command)
                print(self.master)
                for point in self.master.get_agent().point_definitions.all_points():
                    # print("point",point)
                    # print("y",y)
                    if command.get("object") == point.measurement_id and point.value != command.get("value"):
                        print("point", point)
                        point.magnitude = command.get("value")
                        print(opendnp3.Binary(point.value), point.index)
                        self.master.apply_update(opendnp3.Binary(point.value), point.index)


            # storing the magnitude and measurement_mRID values to publish in the dnp3 points for measurement key values
            for y in measurement_values:
                # print(self.processor_point_def.points_by_mrid())
                m = measurement_values[y]
                if "magnitude" in m.keys():
                   for point in self.outstation.get_agent().point_definitions.all_points():
                       #print("point",point)
                       #print("y",y)
                       if m.get("measurement_mrid") == point.measurement_id and point.magnitude != m.get("magnitude"):
                           point.magnitude = m.get("magnitude")
                           self.outstation.apply_update(opendnp3.Analog(point.magnitude), point.index)
                elif "value" in m.keys():
                    for point in self.outstation.get_agent().point_definitions.all_points():
                        if m.get("measurement_mrid") == point.measurement_id and point.value != m.get("value"):
                             point.value = m.get("value")
                             self.outstation.apply_update(opendnp3.Binary(point.value), point.index)
        except Exception as e:
            message_str = "An error occurred while trying to translate the  message received" + str(e)

    def assign_val_a(self, data_type, group, variation, index, name, description, measurement_type, measurement_id):
        """ Method is to initialize  parameters to be used for generating  output  points for measurement key values """
        records = dict()  # type: Dict[str, Any]
        records["data_type"] = data_type
        records["index"] = index
        records["group"] = group
        records["variation"] = variation
        records["description"] = description
        records["name"] = name
        records["measurement_type"] = measurement_type
        records["measurement_id"] = measurement_id
        records["magnitude"] = "0"
        self.out_json.append(records)

    def assign_val_d(self, data_type, group, variation, index, name, description, measurement_id, attribute):
        """ This method is to initialize  parameters to be used for generating  output  points for output points"""
        records = dict()
        records["data_type"] = data_type
        records["index"] = index
        records["group"] = group
        records["variation"] = variation
        records["description"] = description
        records["name"] = name
        # records["measurement_type"] = measurement_type
        records["measurement_id"] = measurement_id
        records["attribute"] = attribute
        records["value"] = "0"
        self.out_json.append(records)

    def assign_valc(self, data_type, group, variation, index, name, description, measurement_id, attribute):
        """ Method is to initialize  parameters to be used for generating  dnp3 control as Analog/Binary Input points"""
        records = dict()
        records["data_type"] = data_type
        records["index"] = index
        records["group"] = group
        records["variation"] = variation
        records["description"] = description
        records["name"] = name
        # records["measurement_type"] = measurement_type
        records["attribute"] = attribute
        records["measurement_id"] = measurement_id
        self.out_json.append(records)

    def load_json(self, out_json, out_file):
        with open(out_file, 'w') as fp:
            out_dict = dict({'points': out_json})
            json.dump(out_dict, fp, indent=2, sort_keys=True)

    def load_point_def(self, point_def):
        self.processor_point_def = point_def
        
    def load_outstation(self, outstation):
        self.outstation = outstation

    def load_master(self, master):
        self.master = master

    def _create_dnp3_object_map(self):
        """This method creates the points by taking the input data from model dictionary file"""

        feeders = self.file_dict.get("feeders", [])
        measurements = list()
        capacitors = list()
        regulators = list()
        switches = list()
        solarpanels = list()
        batteries = list()
        fuses = list()
        breakers = list()
        reclosers = list()
        energyconsumers = list()
        for x in feeders:
            measurements = x.get("measurements", [])
            capacitors = x.get("capacitors", [])
            regulators = x.get("regulators", [])
            switches = x.get("switches", [])
            solarpanels = x.get("solarpanels", [])
            batteries = x.get("batteries", [])
            fuses = x.get("fuses", [])
            breakers = x.get("breakers", [])
            reclosers = x.get("reclosers", [])
            energyconsumers = x.get("energyconsumers", [])

        for m in measurements:
            attribute = attribute_map['regulators']['attribute']
            measurement_type = m.get("measurementType")
            measurement_id = m.get("mRID")
            name= measurement_id.replace('-', '').replace('_', '')
            description = "Name:" + m['name'] + ",Phase:" + m['phases'] + ",MeasurementType:" + measurement_type + ",ConnectivityNode:" + m.get("ConnectivityNode") +",SimObject:" + m.get("SimObject")
            if m['MeasurementClass'] == "Analog":
                self.assign_val_a("AI", 30, 1, self.c_ai, name, description, measurement_type, measurement_id)
                self.c_ai += 1

            elif m['MeasurementClass'] == "Discrete" and  measurement_type == "Pos":
                if "Reg" in m['name']:
                    for r in range(5, 7):
                        self.assign_val_d("AO", 42, 3, self.c_ao, name, description,  measurement_id, attribute[r])
                        self.c_ao += 1
                else:
                    self.assign_val_a("DI", 1, 2, self.c_di, name, description, measurement_type, measurement_id)
                    self.c_di += 1


        for m in capacitors:
            measurement_id = m.get("mRID")
            cap_attribute = attribute_map['capacitors']['attribute']  # type: List[str]

            for l in range(0, 4):
                # publishing attribute value for capacitors as Bianry/Analog Input points based on phase  attribute
                name = uuid.uuid4().hex
                description = "Name:" + m['name'] + "ConductingEquipment_type:LinearShuntCompensator" + ",Attribute:" + cap_attribute[l]  + ",Phase:" + m['phases']
                self.assign_val_d("AO", 42, 3, self.c_ao, name, description, measurement_id, cap_attribute[l])
                self.c_ao += 1
            for p in range(0, len(m['phases'])):
                name =uuid.uuid4().hex
                description = "Name:" + m['name'] + ",ConductingEquipment_type:LinearShuntCompensator" + ",controlAttribute:" + cap_attribute[p] + ",Phase:" + m['phases'][p]
                # description = "Capacitor, " + m['name'] + "," + "phase -" + m['phases'][p] + ", and attribute is - " + cap_attribute[4]
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, cap_attribute[4])
                self.c_do += 1

        for m in regulators:
            reg_attribute = attribute_map['regulators']['attribute']
            # bank_phase = list(m['bankPhases'])
            for n in range(0, 4):
                measurement_id = m.get("mRID")
                name = uuid.uuid4().hex
                description = "Name:" + m['bankName'] + ",ConductingEquipment_type:RatioTapChanger_Reg" +",Phase:" + m['bankPhases'] + ",Attribute:" + reg_attribute[n]
                self.assign_val_d("AO", 42, 3, self.c_ao, name, description, measurement_id[0], reg_attribute[n])
                self.c_ao += 1
            for i in range(5, 7):
                for j in range(0, len(m['bankPhases'])):
                    measurement_id = m.get("mRID")[j]
                    name = uuid.uuid4().hex
                    description = "Name:" + m['tankName'][j] + ",ConductingEquipment_type:RatioTapChanger_Reg"+ ",Phase:" + m['bankPhases'][j] + ",controlAttribute:" + reg_attribute[i]
                    self.assign_val_d("AO", 42, 3, self.c_ao, name, description, measurement_id,reg_attribute[i])
                    self.c_ao += 1

        for m in solarpanels:
            measurement_id = m.get("mRID")
            name = uuid.uuid4().hex
            description = "Solarpanel:" + m['name'] + ",Phase:" + m['phases'] + ",measurementID:" + measurement_id
            self.assign_val_a("AO", 42, 3, self.c_ao, name, description, None, measurement_id)
            self.c_ao += 1

        for m in batteries:
            measurement_id = m.get("mRID")
            name = uuid.uuid4().hex
            description = "Battery, " + m['name'] + ",Phase: " + m['phases'] + ",ConductingEquipment_type:PowerElectronicConnections"
            self.assign_val_a("AO", 42, 3, self.c_ao, name, description, None, measurement_id)
            self.c_ao += 1

        for m in switches:
            measurement_id = m.get("mRID")
            switch_attribute = attribute_map['switches']['attribute']
            for k in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = uuid.uuid4().hex
                description = "Name:" + m["name"] + ",ConductingEquipment_type:LoadBreakSwitch" + "Phase:" + phase_value[k] +",controlAttribute:"+switch_attribute
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, switch_attribute)
                self.c_do += 1

        for m in fuses:
            measurement_id = m.get("mRID")
            switch_attribute = attribute_map['switches']['attribute']
            for l in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = uuid.uuid4().hex
                description = "Name:" + m["name"] + ",Phase:" + phase_value[l] + ",Attribute:" + switch_attribute + ",mRID" + measurement_id
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, switch_attribute)
                self.c_do += 1

        for m in breakers:
            measurement_id = m.get("mRID")
            switch_attribute = attribute_map['switches']['attribute']
            for n in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name =uuid.uuid4().hex
                description = "Name: " + m["name"] + ",Phase:" + phase_value[n] + ",ConductingEquipment_type:Breaker" + ",controlAttribute:" + switch_attribute
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, switch_attribute)
                self.c_do += 1

        for m in reclosers:
            measurement_id = m.get("mRID")
            switch_attribute = attribute_map['switches']['attribute']
            for k in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = uuid.uuid4().hex
                description = "Recloser, " + m["name"] + "Phase: - " + phase_value[k] + ",ConductingEquipment_type:Recloser"+"controlAttribute:" + switch_attribute
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, switch_attribute)
                self.c_do += 1


        for m in energyconsumers:
            measurement_id = m.get("mRID")
            for k in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = uuid.uuid4().hex
                description = "EnergyConsumer, " + m["name"] + "Phase: - " + phase_value[k] 
                self.assign_val_a("AI", 30, 1, self.c_ai, name, description, None , measurement_id)
                self.c_ai += 1

        return self.out_json

