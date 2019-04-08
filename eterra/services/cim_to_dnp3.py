import json
import logging
import sys
import time

from gridappsd.topics import fncs_input_topic, fncs_output_topic
from typing import List, Dict, Union

out_json = list()

# attribute mapping for control points
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

}  # type: Dict[str, Union[Dict[str, List[str]], Dict[str, str]]]


class dnp3_mapping():

    def __init__(self, map_file):
        self.c_ao = 0
        self.c_bo = 0
        self.c_ai = 0
        self.c_bi = 0
        with open(map_file, 'r') as f:
            self.file_dict = json.load(f)

        self.out_json = list()
		
	 def on_message(self, headers, msg):
        message = {}
       
		message_str = 'translating following message for fncs simulation '+simulation_id+' '+str(goss_message)
		json_msg = yaml.safe_load(str(msg))
		print(message_str)
		
		if simulation_id == None or simulation_id == '' or type(simulation_id) != str:
			raise ValueError(
				'simulation_id must be a nonempty string.\n'
				+ 'simulation_id = {0}'.format(simulation_id))
		if goss_message == None or goss_message == '' or type(goss_message) != str:
			raise ValueError(
				'goss_message must be a nonempty string.\n'
				+ 'goss_message = {0}'.format(goss_message))
		try:
        goss_message_format = yaml.safe_load(goss_message)
        if type(goss_message_format) != dict:
            raise ValueError(
                'goss_message is not a json formatted string.'
                + '\ngoss_message = {0}'.format(goss_message))
        fncs_input_topic = '{0}/fncs_input'.format(simulation_id)
        fncs_input_message = {"{}".format(simulation_id) : {}}
		measurement_values = goss_message_format["message"]["measurements"]
			for y in measurement_values:
				magnitude = y.get["magnitude"]
				measurement_mRID = y.get["measurement_mrid"]
		
	 def assign_val_m(self, data_type, group, variation, index, name, description, measurement_type, measurement_id,magnitude): # function for measurement key values
        records = dict()
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
		# function for ouput points
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
        # type: (object, char, int, int, object, object, object, object) -> object
        # for control input points -capacitors, switches,regulators
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
        feeders = self.file_dict.get("feeders", [])
        for x in feeders:  # reading model.dict json file
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
                self.assign_val_m("AO", 42, 3, self.c_ao, name, description, measurement_type, measurement_id, magnitude)
                self.c_ao += 1
            elif m['MeasurementClass'] == "Discrete" and measurement_mRID == measurement_id:
                self.assign_val_m("BO", 11, 1, self.c_bo, name, description, measurement_type, measurement_id, magnitude)
                self.c_bo += 1

        for m in capacitors:
            object_id = m.get("mRID")
            name = m.get("name")
            phase_value = list(m['phases'])
            description1 = "Capacitor, " + m['name'] + "," + "phase -" + m['phases']
            cap_attribute = attribute_map['capacitors']['attribute']  # type: List[str]
            for l in range(0, 4):
                self.assign_valc("AI", 32, 3, self.c_ai, name, description1, object_id,
                                 attribute_map['capacitors']['attribute'][l])
                self.c_ai += 1
                for j in range(0, len(m['phases'])):
                    description = "Capacitor, " + m['name'] + "," + "phase -" + phase_value[j]
                    self.assign_valc("BI", 2, 1, self.c_bi, name, description, object_id,
                                     attribute_map['capacitors']['attribute'][4])
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