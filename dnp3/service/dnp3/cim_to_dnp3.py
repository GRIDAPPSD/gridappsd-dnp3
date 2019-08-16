import json
import yaml
import sys
import datetime
import random

from pydnp3 import opendnp3
from typing import List, Dict, Union, Any
from dnp3.outstation import DNP3Outstation
from dnp3.points import (
    PointArray, PointDefinitions, PointDefinition, DNP3Exception, POINT_TYPE_ANALOG_INPUT, POINT_TYPE_BINARY_INPUT
)

out_json = list()

'''Dictionary for mapping the attribute values of control poitns for Capacitor, Regulator and Switches'''

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
                      "TapChanger.lineDropX"]}

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


    def on_message(self, simulation_id,message):
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

            json_msg = yaml.safe_load(str(message))

            if type(json_msg) != dict:
                raise ValueError(
                    ' is not a json formatted string.'
                    + '\njson_msg = {0}'.format(json_msg))

            # fncs_input_message = {"{}".format(simulation_id): {}}
            measurement_values = json_msg["message"]["measurements"]

            # storing the magnitude and measurement_mRID values to publish in the dnp3 points for measurement key values
            for y in measurement_values:
                if "magnitude" in y.keys():
                    for point in self.outstation.get_agent().point_definitions.all_points():
                        if y.get("measurement_mrid") == point.measurement_id and point.magnitude != y.get("magnitude"):
                             point.magnitude = y.get("magnitude")
                             self.outstation.apply_update(opendnp3.Analog(point.magnitude), point.index)
                elif "value" in y.keys():
                    for point in self.outstation.get_agent().point_definitions.all_points():
                        if y.get("measurement_mrid") == point.measurement_id and point.value != y.get("value"):
                             point.value = y.get("value")
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

        measurement_index = {}
        for m in measurements:
            measurement_type = m.get("measurementType")
            measurement_id = m.get("mRID")
            namelist = m.get("name").split('_')
            if namelist[0] in measurement_index:
                measurement_index[namelist[0]] = measurement_index[namelist[0]] + 1
            else:
                measurement_index[namelist[0]] = 0
            name = namelist[0] + str(measurement_index[namelist[0]])
            description = m['name'] + "," + "and phase-" + m['phases'] + "_" + "and mrid - " + measurement_id
            if m['MeasurementClass'] == "Analog":
                self.assign_val_a("AI", 30, 1, self.c_ai, name, description, measurement_type, measurement_id)
                self.c_ai += 1

            if m['MeasurementClass'] == "Discrete":
                self.assign_val_a("DI", 1, 2, self.c_di, name, description, measurement_type, measurement_id)
                self.c_di += 1

        capacitor_index = {}
        for m in capacitors:
            measurement_id = m.get("mRID")
            cap_attribute = attribute_map['capacitors']['attribute']  # type: List[str]

            for l in range(0, 4):
                # publishing attribute value for capacitors as Bianry/Analog Input points based on phase  attribute
                attlist = cap_attribute[l].split('.')
                name = m.get("name") + "_" + attlist[0]
                if name in capacitor_index:
                    capacitor_index[name] = capacitor_index[name] + 1
                else:
                    capacitor_index[name] = 0
                name = name + "_" + str(capacitor_index[name])
                description = "Capacitor, " + m['name'] + "," + "phase -" + m['phases'] + ", and attribute is - " + cap_attribute[l]
                self.assign_val_d("AO", 42, 3, self.c_ao, name, description, measurement_id, cap_attribute[l])
                self.c_ao += 1

            for p in range(0, len(m['phases'])):
                attlist = cap_attribute[4].split('.')
                name = m.get("name") + "_" + attlist[0]
                if name in capacitor_index:
                    capacitor_index[name] = capacitor_index[name] + 1
                else:
                    capacitor_index[name] = 0
                name = name + "_" + str(capacitor_index[name])
                description = "Capacitor, " + m['name'] + "," + "phase -" + m['phases'][p] + ", and attribute is - " + cap_attribute[4]
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, cap_attribute[4])
                self.c_do += 1

        regulator_index = {}
        for m in regulators:
            reg_attribute = attribute_map['regulators']['attribute']
            bank_phase = list(m['bankPhases'])
            for n in range(0, 4):
                measurement_id = list(m.get("mRID"))
                attlist = reg_attribute[n].split('.')
                name =  m.get("bankName") + "_" + attlist[0]
                if name in regulator_index:
                    regulator_index[name] = regulator_index[name] + 1
                else:
                    regulator_index[name] = 0
                name = name + "_" + str(regulator_index[name])
                description = "Regulator, " + m['bankName'] + " " + "phase is  -  " + m['bankPhases'] + ", Attribute is - " + reg_attribute[n]
                self.assign_val_d("AO", 42, 3, self.c_ao, name, description, measurement_id[0], reg_attribute[n])
                self.c_ao += 1
                
            for i in range(4, 7):
                for j in range(0, len(m['bankPhases'])):
                    attlist = reg_attribute[i].split('.')
                    name = m.get("bankName") + "_" + attlist[0]
                    if name in regulator_index:
                        regulator_index[name] = regulator_index[name] + 1
                    else:
                        regulator_index[name] = 0
                    name = name + "_" + str(regulator_index[name])
                    description = "Regulator, " + m['tankName'][j] + " " "phase is  -  " + m['bankPhases'][j] + ", Attribute is - " + reg_attribute[i]
                    measurement_id = m.get("mRID")
                    #name = m['tankName'][j] + "_" + reg_phase_attribute + "_" + m['bankPhases'][j]
                    self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id[j],reg_attribute[i])
                    self.c_do += 1

        solarpanel_index = {}
        for m in solarpanels:
            measurement_id = m.get("mRID")
            name = "Solar_" + m.get("name") + "_" + m['phases']
            if name in solarpanel_index:
                solarpanel_index[name] = solarpanel_index[name] + 1
            else:
                solarpanel_index[name] = 0
            name = name + "_" + str(solarpanel_index[name])
            description = "Solarpanel " + m['name'] + "phases - " + m['phases']
            self.assign_val_a("AO", 42, 3, self.c_ao, name, description, None, measurement_id)
            self.c_ao += 1

        battery_index = {}
        for m in batteries:
            measurement_id = m.get("mRID")
            name = "Battery_" + m.get("name") + "_" + m['phases']
            if name in battery_index:
                battery_index[name] = battery_index[name] + 1
            else:
                battery_index[name] = 0
            name = name + "_" + str(battery_index[name])
            description = "Battery, " + m['name'] + "phases - " + m['phases']
            self.assign_val_a("AO", 42, 3, self.c_ao, name, description, None, measurement_id)
            self.c_ao += 1

        for m in switches:
            measurement_id = m.get("mRID")
            switch_attribute = attribute_map['switches']['attribute']

            for k in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = "Switch" + "_"+ m.get("name") + "_"  + phase_value[k]
                description = "Switch, " + m["name"] + "phases - " + phase_value[k]
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, switch_attribute)
                self.c_do += 1

        for m in fuses:
            measurement_id = m.get("mRID")
            switch_attribute = attribute_map['switches']['attribute']
            name = m.get("name") + "_" + m['phases'] + "_" + switch_attribute
            for l in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = "Fuse" +"_"+ m.get("name") + "_" + phase_value[l]
                description = "Fuse, " + m["name"] + "phases - " + phase_value[l]
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, switch_attribute)
                self.c_do += 1

        for m in breakers:
            measurement_id = m.get("mRID")
            switch_attribute = attribute_map['switches']['attribute']
            for n in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = "Breaker"+"_"+m.get("name")  + "_" + phase_value[n]
                description = "Breaker, " + m["name"] + "phases - " + phase_value[n]
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, switch_attribute)
                self.c_do += 1

        for m in reclosers:
            measurement_id = m.get("mRID")
            switch_attribute = attribute_map['switches']['attribute']
            for k in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = "Recloser"+"_"+m.get("name") + "_" + phase_value[k]
                description = "Recloser, " + m["name"] + "phases - " + phase_value[k]
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, switch_attribute)
                self.c_do += 1

        return self.out_json

