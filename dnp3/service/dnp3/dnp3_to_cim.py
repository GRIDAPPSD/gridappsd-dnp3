import json
import pandas as pd

def build_conversion(DNP3_device_xlsx):
    df = pd.read_excel(r'DNP3 list.xlsx', sheet_name='Shark')
    conversion_dict = {}
    df = df.set_index('Index')
    shark_dict = df.T.to_dict()
    conversion_dict['Shark'] = shark_dict
    conversion_dict = {"Shark": shark_dict}
    with open("conversion_dict.json", "w") as f:
        json.dump(conversion_dict, f, indent=2)

def model_line_dict(model_dict_json):
    from_node = '632'
    to_node = '633'
    node_name = '633'
    line_name = from_node + to_node

    with open("model_dict.json") as f:
        model_dict = json.load(f)

    model_line_dict = {from_node + to_node: {}}
    device_type = "Shark"
    if device_type == 'Shark':
        for meas in model_dict['feeders'][0]['measurements']:
            if meas['name'].startswith('ACLineSegment_' + line_name):
                print(meas)
                if meas['measurementType'] == 'PNV':
                    model_line_dict[from_node + to_node]['Voltage feedback ' + meas['phases'] + ' (L-N)'] = {
                        'mrid': meas['mRID'], 'type': 'magnitude'}
                    model_line_dict[from_node + to_node]['Voltage feedback ' + meas['phases'] + ' (L-L)'] = {
                        'mrid': meas['mRID'], 'type': 'angle'}
                if meas['measurementType'] == 'VA':
                    model_line_dict[from_node + to_node]['P ' + meas['phases']] = {'mrid': meas['mRID'],
                                                                                   'type': 'magnitude'}
                    model_line_dict[from_node + to_node]['Q ' + meas['phases']] = {'mrid': meas['mRID'],
                                                                                   'type': 'angle'}
        #     if meas['ConnectivityNode'] == node_name and meas['ConductingEquipment_type'] == 'ACLineSegment':
        #         print(meas)
    if device_type == 'Capacitor':
        pass
    if device_type == 'Regulator':
        pass
    model_line_dict
    with open("model_line_dict.json", "w") as f:
        json.dump(model_line_dict, f, indent=2)

class CIMMapping():
    """ This creates dnp3 input and output points for incoming CIM messages  and model dictionary file respectively."""

    def __init__(self, conversion_dict="conversion_dict.json", model_line_dict="model_line_dict.json"):
        with open(conversion_dict) as f:
            #     json.dump(shark_dict,f)
            conversion_dict = json.load(f)
        self.conversion_dict = conversion_dict

        with open(model_line_dict) as f:
            model_line_dict = json.load(f)
        self.model_line_dict = model_line_dict

if __name__ == '__main__':
    CIMMapping()