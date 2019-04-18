import json
import logging
import sys
import time
import yaml


from gridappsd.topics import fncs_input_topic, fncs_output_topic
from typing import List, Dict, Union, Any

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


class dnp3_mapping():
    """ This class creates dnps input and ouput points for incoming CIM messages and data from fncs ouput topic and model dictionary file respectively. It reads the input from model dictionary file created every time when the simulation is run. Each dictiobary file has simulation ID.  """
    def __init__(self, map_file):
        self.c_ao = 0
        self.c_bo = 0
        self.c_ai = 0
        self.c_bi = 0
        with open(map_file, 'r') as f:
            self.file_dict = json.load(f)

        self.out_json = list()
        
    def on_message(self, headers, msg):
        """ This method handles incoming messages on the fncs_output_topic for the simulation_id.
        Parameters
        ----------
        headers: dict
            A dictionary of headers that could be used to determine topic of origin and
            other attributes.
        message: object
            A data structure following the protocol defined in the message structure
            of ``GridAPPSD``.  Most message payloads will be serialized dictionaries, but that is
            not a requirement.
        """
        message = {}
        try:
            message_str = 'received message '+str(msg)

            if simulation_id == None or simulation_id == '' or type(simulation_id) != str:
                raise ValueError(
                    'simulation_id must be a nonempty string.\n'
                    + 'simulation_id = {0}'.format(simulation_id))

            json_msg = yaml.safe_load(str(msg))


            if goss_message == None or goss_message == '' or type(goss_message) != str:
                raise ValueError(
                    'goss_message must be a nonempty string.\n'
                    + 'goss_message = {0}'.format(goss_message))

            goss_message_format = yaml.safe_load(goss_message)

            if type(goss_message_format) != dict:
                raise ValueError(
                    'goss_message is not a json formatted string.'
                    + '\ngoss_message = {0}'.format(goss_message))

            measurement_values = goss_message_format["message"]["measurements"]

            #We are storing the magnitude and measurement_mRID values to publish in the dnp3 points for measurement key values of model dictionary file.
            for y in measurement_values:
                magnitude_value = y.get["magnitude"]
                measurement_mRID = y.get["measurement_mrid"]


        except Exception as ex:
            _send_simulation_status("ERROR", "An error occured while trying to translate the  message received", "ERROR")
            _send_simulation_status("ERROR", str(ex), "ERROR")
        
    def assign_val_m(self, data_type, group, variation, index, name, description, measurement_type, measurement_id,magnitude):
        """ This method is to initialize  parameters to be used for generating  output  points for measurement key values """
        records = dict()  # type: Dict[str, Any]
        records["data_type"] = data_type
        records["index"] = index
        records["group"] = group
        records["variation"] = variation
        records["description"] = description
        records["name"] = name
        records["measurement_type"] = measurement_type
        records["measurement_id"] = measurement_id
        records["magnitude"] = magnitude
        self.out_json.append(records)
           
    def assign_val(self, data_type, group, variation, index, name, description, measurement_type, measurement_id):
        """ This method is to initialize  parameters to be used for generating  output  points for output points"""
        records = dict()
        records["data_type"] = data_type
        records["index"] = index
        records["group"] = group
        records["variation"] = variation
        records["description"] = description
        records["name"] = name
        records["measurement_type"] = measurement_type
        records["measurement_id"] = measurement_id
        self.out_json.append(records)

    def assign_valc(self, data_type, group, variation, index, name, description, object_id, attribute):
        """ This method is to initialize  parameters to be used for  generating  dnp3 control/command as Analog/Binary Input points"""
        records = dict()
        records["data_type"] = data_type
        records["index"] = index
        records["group"] = group
        records["variation"] = variation
        records["description"] = description
        records["name"] = name
        records["object_id"] = object_id
        records["attribute"] = attribute
        self.out_json.append(records)

    def load_json(self, out_json, out_file):
        with open(out_file, 'w') as fp:
            out_dict = dict({'points': out_json})
        json.dump(out_dict, fp, indent=2, sort_keys=True)

    def _create_dnp3_object_map(self):
        """This method creates the points by taking the input data from model dictionary file"""
        feeders = self.file_dict.get("feeders", [])
        for x in feeders:
            measurements = x.get("measurements", [])
            capacitors = x.get("capacitors", [])
            regulators = x.get("regulators", [])
            switches = x.get("switches", [])
            solarpanels = x.get("solarpanels", [])
            batteries = x.get("batteries", [])

        for m in measurements:
            measurement_type = m.get("measurementType")
            measurement_id = m.get("mRID")
            name = m.get("name")
            description = "Equipment is " + m['name'] + "," + m['ConductingEquipment_type'] + " and phase is " + m[
                'phases']

            if m['MeasurementClass'] == "Analog" and measurement_mRID == measurement_id:
                #Checking if magnitude value in CIM message from output topic has a null value
                if(magnitude_value == Null):
                    self.assign_val("AO", 42, 3, self.c_ao, name, description, measurement_type, measurement_id)
                else:
                    self.assign_val_m("AO", 42, 3, self.c_ao, name, description, measurement_type, measurement_id, magnitude)
            self.c_ao += 1
            if m['MeasurementClass'] == "Discrete" and measurement_mRID == measurement_id:
                if (magnitude_value == Null):
                    self.assign_val("BO", 11, 1, self.c_bo, name, description, measurement_type, measurement_id) # print the magnitude value if its not null
                else:
                    self.assign_val_m("BO", 11, 1, self.c_bo, name, description, measurement_type, measurement_id,
                                      magnitude)
             self.c_bo += 1

        for m in capacitors:
            object_id = m.get("mRID")
            name = m.get("name")
            phase_value = list(m['phases'])
            description1 = "Capacitor, " + m['name'] + "," + "phase -" + m['phases']
            cap_attribute = attribute_map['capacitors']['attribute']  # type: List[str]
            for l in range(0, 4):
                # publishing attribute value for capacitors as Bianry/Analog Input points based on phase dependent attribute_map values
                self.assign_valc("AI", 32, 3, self.c_ai, name, description1, object_id,cap_attribute[l])
                self.c_ai += 1
                for j in range(0, len(m['phases'])):
                    description = "Capacitor, " + m['name'] + "," + "phase -" + phase_value[j]
                    self.assign_valc("BI", 2, 1, self.c_bi, name, description, object_id,cap_attribute[4])
                    self.c_bi += 1

        for m in solarpanels:
            measurement_id = m.get("mRID")
            name = m.get("name")
            description = "Solarpanel " + m['name'] + "phases - " + m['phases']
            self.assign_val("AI", 32, 3, self.c_ai, name, description, None, measurement_id)
            self.c_ai += 1

        for m in batteries:
            measurement_id = m.get("mRID")
            name = m.get("name")
            description = "Battery, " + m['name'] + "phases - " + m['phases']
            self.assign_val("AI", 32, 3, self.c_ai, name, description, None, measurement_id)
            self.c_ai += 1

        for m in switches:
            object_id = m.get("mRID")
            name = m.get("name")
            for k in range(0, len(m['phases'])):
                phase_value = list(m['phases'])
                description = "Switch, " + m["name"] + "phases - " + phase_value[k]
                self.assign_valc("BI", 2, 1, self.c_bi, name, description, object_id,
                                 attribute_map['switches']['attribute'])
                self.c_bi += 1

        for m in regulators:
            name = m.get("bankName")
            reg_attribute = attribute_map['regulators']['attribute']
            bank_phase = list(m['bankPhases'])
            description = "Regulator, " + m['bankName'] + " " + "phase is  -  " + m['bankPhases']
            for n in range(0, 5):
                object_id = m.get("mRID")
                self.assign_valc("AI", 32, 3, self.c_ai, name, description, object_id[0], reg_attribute[n])
                self.c_ai += 1
                for i in range(4, 7):
                    reg_phase_attribute = attribute_map['regulators']['attribute'][i]
                for j in range(0, len(m['bankPhases'])):
                    description = "Regulator, " + m['tankName'][j] + " " "phase is  -  " + m['bankPhases'][j]
                    object_id = m.get("mRID")
                    self.assign_valc("AI", 32, 3, self.c_ai, name, description, object_id[j], reg_phase_attribute)
                    self.c_ai += 1

        return self.out_json


def _main(simulation_id,  measurement_map_dir=''):

    measurement_map_file=str(measurement_map_dir)+"model_dict.json"
    _create_dnp3_object_map(measurement_map_file)
    _keep_alive(is_realtime)

def _get_opts():
    parser = argparse.ArgumentParser()
    parser.add_argument("simulation_id", help="The simulation id to use for responses on the message bus.")
    opts = parser.parse_args()
    return opts

if __name__ == "__main__":
    opts = _get_opts()
    simulation_id = opts.simulation_id
    sim_dir = opts.simulation_directory
    _main(simulation_id, sim_dir)
    debugFile.close()