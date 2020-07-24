import json
import yaml
import sys
import datetime
import random
import uuid
import math
# import pydevd;pydevd.settrace(suspend=False) # Uncomment For Debugging on other Threads

from collections import defaultdict
from pydnp3 import opendnp3, openpal, asiopal, asiodnp3
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
        self.c_var = 0
        self.measurements = dict()
        self.out_json = list()
        self.file_dict = map_file
        self.processor_point_def = PointDefinitions()
        self.outstation = DNP3Outstation('',0,'')

    # This is the old on_message. Should probably be removed, unless the "start_service.py" needs it. Leaving this up to PNNL to decide :)
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
                # print(self.processor_point_def.points_by_mrid())
                m = measurement_values[y]
                if "magnitude" in m.keys():
                   for point in self.outstation.get_agent().point_definitions.all_points():
                       #print("point",point)
                       #print("y",y)
                       if m.get("measurement_mrid") == point.measurement_id and point.magnitude != m.get("magnitude"):
                           point.magnitude = m.get("magnitude")
                           self.outstation.apply_update(opendnp3.Analog(point.magnitude), point.index)

                       elif point.measurement_type == "VA" and "VAR" in point.name:
                           angle = math.radians(m.get("angle"))
                           point.magnitude = math.sin(angle) * m.get("magnitude")
                           self.outstation.apply_update(opendnp3.Analog(point.magnitude), point.index)
                       
                       elif point.measurement_type == "VA" and "Watts"  in point.name:
                           angle1 = math.radians(m.get("angle"))
                           point.magnitude = math.cos(angle1) * m.get("magnitude")
                           self.outstation.apply_update(opendnp3.Analog(point.magnitude), point.index)
                       
                       elif point.measurement_type == "VA" and "angle"  in point.name:
                           angle2 = math.radians(m.get("angle"))
                           #point.magnitude = math.cos(angle1) * m.get("magnitude")
                           self.outstation.apply_update(opendnp3.Analog(angle2), point.index)
                           
                       
                elif "value" in m.keys():
                    for point in self.outstation.get_agent().point_definitions.all_points():
                        if m.get("measurement_mrid") == point.measurement_id and point.value != m.get("value"):
                             point.value = m.get("value")
                             self.outstation.apply_update(opendnp3.Binary(point.value), point.index)
        except Exception as e:
            message_str = "An error occurred while trying to translate the  message received" + str(e)

    # create_message_updates is called in new_start_service.py (in the new on_message method).
    def create_message_updates(self, simulation_id, message):
        """ This method creates an atomic "updates" object for any outstation to consume via their .Apply method.
        ----------
        headers: dict
            A dictionary of headers that could be used to determine topic of origin and
            other attributes.
        message: object

        """
        try:
            message_str = 'received message ' + str(message)

            builder = asiodnp3.UpdateBuilder()
            json_msg = yaml.safe_load(str(message))

            if type(json_msg) != dict:
                raise ValueError(
                    ' is not a json formatted string.'
                    + '\njson_msg = {0}'.format(json_msg))

            # fncs_input_message = {"{}".format(simulation_id): {}}
            measurement_values = json_msg["message"]["measurements"]

            # Calculate each net-phase measurement
            if(measurement_values):

                myPoints = self.outstation.get_agent().point_definitions.all_points()
                netPoints = list(filter(lambda x: "net-" in x.description, myPoints))
                
                for point in netPoints:
                    ptMeasurements = list(filter(lambda m: m.get("measurement_mrid") in point.measurement_id, measurement_values.values()))
                    netValue = 0.0
                    
                    for m in ptMeasurements:
                        if "VAR" in point.name:
                            angle = math.radians(m.get("angle"))
                            netValue = netValue + math.sin(angle) * float(m.get("magnitude"))

                        elif "Watts" in point.name:
                            angle = math.radians(m.get("angle"))
                            netValue += math.cos(angle) * float(m.get("magnitude"))

                        else:
                            netValue += float(m.get("magnitude"))

                    point.magnitude = netValue
                    builder.Update(opendnp3.Analog(point.magnitude), point.index)

            # Calculate each measurement
            for y in measurement_values:
                # print(self.processor_point_def.points_by_mrid())
                m = measurement_values[y]

                if "magnitude" in m.keys():
                   for point in self.outstation.get_agent().point_definitions.all_points():
                       #print("point",point.name)
                       #print("y",y)
                        if m.get("measurement_mrid") == point.measurement_id:
                            if point.magnitude != float(m.get("magnitude")):
                                point.magnitude = float(m.get("magnitude"))
                                builder.Update(opendnp3.Analog(point.magnitude), point.index)

                            if point.measurement_type == "VA":
                                if "VAR" in point.name:
                                    angle = math.radians(m.get("angle"))
                                    point.magnitude = math.sin(angle) * float(m.get("magnitude"))
                                    builder.Update(opendnp3.Analog(point.magnitude), point.index)

                                if "Watts" in point.name:
                                    angle1 = math.radians(m.get("angle"))
                                    point.magnitude = math.cos(angle1) * float(m.get("magnitude"))
                                    builder.Update(opendnp3.Analog(point.magnitude), point.index)

                                if  "angle" in point.name:
                                    angle2 = math.radians(m.get("angle"))
                                    builder.Update(opendnp3.Analog(angle2), point.index)

                elif "value" in m.keys():
                    for point in self.outstation.get_agent().point_definitions.all_points():
                        if m.get("measurement_mrid") == point.measurement_id and point.value != m.get("value"):
                             point.value = m.get("value")
                             builder.Update(opendnp3.Binary(point.value), point.index)

            # Return the atomic "updates" object
            print("Updates Created")
            return builder.Build()
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

        # Unique grouping of measurements - GroupBy Name, Type and Connectivity node
        groupByNameTypeConNode = defaultdict(list) 
        for m in measurements:
            groupByNameTypeConNode[m['name']+m.get("measurementType")+m.get("ConnectivityNode")].append(m)

        # Create Net Phase DNP3 Points
        for grpM in groupByNameTypeConNode.values():

            if grpM[0]['MeasurementClass'] == "Analog" and grpM[0].get("measurementType") == "VA":
                measurement_type = grpM[0].get("measurementType")
                measurement_id = grpM[0].get("mRID") +","+ grpM[1].get("mRID") +","+ grpM[2].get("mRID")
                

                name1 = grpM[0]['name'] + '-' + "Phases:ABC" +  '-net-VAR-value'
                name2 = grpM[0]['name'] + '-' + "Phases:ABC" +  '-net-Watts-value'
                name3 = grpM[0]['name'] + '-' + "Phases:ABC" +  '-net-VA-value'

                description1 = "Name:" + grpM[0]['name'] + ",MeasurementType:" + "net-VAR" + ",ConnectivityNode:" + grpM[0].get("ConnectivityNode") +",SimObject:" + grpM[0].get("SimObject")
                description2 = "Name:" + grpM[0]['name'] + ",MeasurementType:" + "net-Watts" + ",ConnectivityNode:" + grpM[0].get("ConnectivityNode") +",SimObject:" + grpM[0].get("SimObject")
                description3 = "Name:" + grpM[0]['name'] + ",MeasurementType:" + "net-VA" + ",ConnectivityNode:" + grpM[0].get("ConnectivityNode") +",SimObject:" + grpM[0].get("SimObject")

                self.assign_val_a("AI", 30, 1, self.c_ai, name1, description1, measurement_type, measurement_id)
                self.c_ai += 1
                self.assign_val_a("AI", 30, 1, self.c_ai, name2, description2, measurement_type, measurement_id)
                self.c_ai += 1
                self.assign_val_a("AI", 30, 1, self.c_ai, name3, description3, measurement_type, measurement_id)
                self.c_ai += 1

        # Create Each Phase DNP3 Points
        for m in measurements:
            attribute = attribute_map['regulators']['attribute']
            measurement_type = m.get("measurementType")
            measurement_id = m.get("mRID")
            name= m['name'] + '-' + m['phases']
            description = "Name:" + m['name'] + ",Phase:" + m['phases'] + ",MeasurementType:" + measurement_type + ",ConnectivityNode:" + m.get("ConnectivityNode") +",SimObject:" + m.get("SimObject")
            if m['MeasurementClass'] == "Analog":
                self.assign_val_a("AI", 30, 1, self.c_ai, name, description, measurement_type, measurement_id)
                self.c_ai += 1

                if m.get("measurementType") == "VA":
                    measurement_id = m.get("mRID")
                    name1 = m['name'] + '-' + m['phases'] +  '-VAR-value'
                    name2 = m['name'] + '-' + m['phases'] + '-Watts-value'
                    name3 = m['name'] + '-' + m['phases'] + '-angle'

                    description1 = "Name:" + m['name'] + ",Phase:" + m['phases'] + ",MeasurementType:" + "VAR" + ",ConnectivityNode:" + m.get("ConnectivityNode") +",SimObject:" + m.get("SimObject")
                    description2 = "Name:" + m['name'] + ",Phase:" + m['phases'] + ",MeasurementType:" + "Watt" + ",ConnectivityNode:" + m.get("ConnectivityNode") +",SimObject:" + m.get("SimObject")
                    description3 = "Name:" + m['name'] + ",Phase:" + m['phases'] + ",MeasurementType:" + "angle" + ",ConnectivityNode:" + m.get("ConnectivityNode") + ",SimObject:" + m.get("SimObject")
                    if m['MeasurementClass'] == "Analog":
                        self.assign_val_a("AI", 30, 1, self.c_ai, name1, description1, measurement_type, measurement_id)
                        self.c_ai += 1
                        self.assign_val_a("AI", 30, 1, self.c_ai, name2, description2, measurement_type, measurement_id)
                        self.c_ai += 1
                        self.assign_val_a("AI", 30, 1, self.c_ai, name3, description3, measurement_type, measurement_id)
                        self.c_ai += 1


            elif m['MeasurementClass'] == "Discrete" and  measurement_type == "Pos":
                if "RatioTapChanger" in m['name'] or "reg" in m["SimObject"]:
                    # TODO: Do we need step?
                    for r in range(5, 7): # [r==4]: Step, [r==5]: LineDropR, [r==6]:LineDropX 
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
                name = m['name']
                description = "Name:" + m['name'] + "ConductingEquipment_type:LinearShuntCompensator" + ",Attribute:" + cap_attribute[l]  + ",Phase:" + m['phases']
                self.assign_val_d("AO", 42, 3, self.c_ao, name, description, measurement_id, cap_attribute[l])
                self.c_ao += 1
            for p in range(0, len(m['phases'])):
                name = m['name'] + m['phases'][p]
                description = "Name:" + m['name'] + ",ConductingEquipment_type:LinearShuntCompensator" + ",controlAttribute:" + cap_attribute[p] + ",Phase:" + m['phases'][p]
                # description = "Capacitor, " + m['name'] + "," + "phase -" + m['phases'][p] + ", and attribute is - " + cap_attribute[4]
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, cap_attribute[4])
                self.c_do += 1

        for m in regulators:
            reg_attribute = attribute_map['regulators']['attribute']
            # bank_phase = list(m['bankPhases'])
            for n in range(0, 4):
                measurement_id = m.get("mRID")
                name = m['bankname'] + '-' + m['bankPhases']
                description = "Name:" + m['bankName'] + ",ConductingEquipment_type:RatioTapChanger_Reg" +",Phase:" + m['bankPhases'] + ",Attribute:" + reg_attribute[n]
                self.assign_val_d("AO", 42, 3, self.c_ao, name, description, measurement_id[0], reg_attribute[n])
                self.c_ao += 1
            for i in range(5, 7):
                for j in range(0, len(m['bankPhases'])):
                    measurement_id = m.get("mRID")[j]
                    name = m['tankName'][j] + '-' + m['bankPhases'][j]
                    description = "Name:" + m['tankName'][j] + ",ConductingEquipment_type:RatioTapChanger_Reg"+ ",Phase:" + m['bankPhases'][j] + ",controlAttribute:" + reg_attribute[i]
                    self.assign_val_d("AO", 42, 3, self.c_ao, name, description, measurement_id,reg_attribute[i])
                    self.c_ao += 1
        
        for m in solarpanels:
            for k in range(0, len(m['phases'])):
                measurement_id = m.get("mRID")
                name = "Solar" + m['name'] + '-' + m['phases'][k] +  '-Watts-value'
                description = "Solarpanel:" + m['name'] + ",Phase:" + m['phases'] + ",measurementID:" + measurement_id
                self.assign_val_d("AO", 42, 3, self.c_ao, name, description, measurement_id, "PowerElectronicsConnection.p")
                self.c_ao += 1
                
                name1 = "Solar" + m['name'] + '-' + m['phases'][k] +  '-VAR-value'
                self.assign_val_d("AO", 42, 3, self.c_ao, name1, description, measurement_id, "PowerElectronicsConnection.q")
                self.c_ao += 1
                
                name2 = "Solar" + m['name'] + '-' + m['phases'][k] +  '-VAR-Net-value'
                self.assign_val_d("AO", 42, 3, self.c_ao, name2, description, measurement_id, "PowerElectronicsConnection.q")
                self.c_ao += 1
                
                name3 = "Solar"+ m['name'] + '-' + m['phases'][k] +  '-Watts-Net-value'
                self.assign_val_d("AO", 42, 3, self.c_ao, name3, description, measurement_id, "PowerElectronicsConnection.p")
                self.c_ao += 1
			
        for m in batteries:
            for l in range(0, len(m['phases'])):
                measurement_id = m.get("mRID")
                name = m['name'] + '-' + m['phases'][l] +  '-Watts-value'
                description = "Battery, " + m['name'][l] + ",Phase: " + m['phases'] + ",ConductingEquipment_type:PowerElectronicConnections"
                self.assign_val_d("AO", 42, 3, self.c_ao, name, description,measurement_id, "PowerElectronicsConnection.p")
                self.c_ao += 1
                name1 = m['name'] + '-' + m['phases'][l] +  '-VAR-value'
                self.assign_val_d("AO", 42, 3, self.c_ao, name1, description,measurement_id, "PowerElectronicsConnection.q")
                self.c_ao += 1
            
        for m in switches:
            measurement_id = m.get("mRID")
            switch_attribute = attribute_map['switches']['attribute']
            for k in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = m['name'] + "Phase:" + m['phases'][k]
                description = "Name:" + m["name"] + ",ConductingEquipment_type:LoadBreakSwitch" + "Phase:" + phase_value[k] +",controlAttribute:"+switch_attribute
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, switch_attribute)
                self.c_do += 1

        for m in fuses:
            measurement_id = m.get("mRID")
            switch_attribute = attribute_map['switches']['attribute']
            for l in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = m['name'] + "Phase:" + m['phases'][l]
                description = "Name:" + m["name"] + ",Phase:" + phase_value[l] + ",Attribute:" + switch_attribute + ",mRID" + measurement_id
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, switch_attribute)
                self.c_do += 1

        for m in breakers:
            measurement_id = m.get("mRID")
            switch_attribute = attribute_map['switches']['attribute']
            for n in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = m['name'] + "Phase:" + m['phases'][n]
                description = "Name: " + m["name"] + ",Phase:" + phase_value[n] + ",ConductingEquipment_type:Breaker" + ",controlAttribute:" + switch_attribute
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, switch_attribute)
                self.c_do += 1
        
        for m in reclosers:
            measurement_id = m.get("mRID")
            switch_attribute = attribute_map['switches']['attribute']
            for i in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = m['name'] + "Phase:" + m['phases'][i]
                description = "Recloser, " + m["name"] + "Phase: - " + phase_value[i] + ",ConductingEquipment_type:Recloser"+"controlAttribute:" + switch_attribute
                self.assign_val_d("DO", 12, 1, self.c_do, name, description, measurement_id, switch_attribute)
                self.c_do += 1

        for m in energyconsumers:
            measurement_id = m.get("mRID")
            for k in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                name = m['name']+"phase:" + m['phases'][k]
                description = "EnergyConsumer, " + m["name"] + "Phase: " + phase_value[k] 
                self.assign_val_d("AO", 42, 3, self.c_ao, name, description, measurement_id, "EnergyConsumer.p")
                self.c_ao += 1
                
                name1 = m['name']+"phase:" + m['phases'][k] + "control"
                self.assign_val_d("DO", 12, 1, self.c_do, name1, description, measurement_id, "EnergyConsumer.p")
                self.c_do += 1

        return self.out_json

