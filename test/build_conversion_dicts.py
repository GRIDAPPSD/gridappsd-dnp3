
import pandas as pd
import json
import numpy as np

def get_conversion_model(csv_file,sheet_name):
    df = pd.read_excel(csv_file,sheet_name=sheet_name)
    df = df.replace(np.nan, '', regex=True)
    master_dict ={
        'Analog input':{},
        'Analog output':{},
        'Binary input':{},
        'Binary output':{}  }

#     temp_df = pd.DataFrame(df[1:4])
#     temp_df = temp_df.set_index('Index')
#     master_dict['Analog input'] = temp_df.T

#     temp_df = pd.DataFrame(df[6:10])
#     temp_df = temp_df.set_index('Index')
#     master_dict['Analog output'] = temp_df.T

    x = []

    for index, row in df.iterrows():
#         print(pd.isna(row['Multiplier']))
        if pd.isna(row['Multiplier']) or row['Multiplier'] == '':
#             print(index)
            x.append(index)
    print(df.shape)
    x.append(df.shape[0])
    it = iter(x)
    for x in it:
#         print(x)
        type_name = df.iloc[x][1]
        print(type_name)
        next_value = next(it)
        print (x+1, next_value)
        print(pd.DataFrame(df[x+1:next_value]))
        temp_df = pd.DataFrame(df[x+1: next_value])
#         temp_df = temp_df.set_index('Index')
#         temp_df = temp_df.replace(np.nan, '', regex=True)
        temp_dict = temp_df.T.to_dict()
        temp_dict_set_key_to_index = {}
        for k,v in temp_dict.items():
            temp_dict_set_key_to_index[int(v['Index'])] = v
        master_dict[type_name] = temp_dict_set_key_to_index
    return master_dict


def convert_rtu(csv_file=r'DNP3_Dict_Feb23(3).xlsx', sheet_name='RTU1'):
    skiprows = 0
    if sheet_name == 'RTU1':
        skiprows = 1
    df = pd.read_excel(csv_file, skiprows=skiprows, sheet_name=sheet_name)
    df = df.replace(np.nan, '', regex=True)
    master_dict = {}
    conversion_name_dict = {}
    phase_dict = {'1': 'A', '2': 'B', '3': 'C'}
    for index, row in df.iterrows():
        rtu_string = sheet_name
        if rtu_string not in master_dict:
            master_dict[rtu_string] = {'Analog input': {}}
        #             conversion_name_dict[rtu_string] = []
        temp_dict = row.to_dict()
        index = temp_dict['Index']
        temp_dict['Index'] = index
        temp_name = temp_dict['Name']
        if not temp_name.strip():
            print('empty')
            break
        if 'LTC_' in temp_name:
            processed_name = temp_name + '_A'  # fake phase added I don't know what to do with this
        else:
            last_underscore_index = temp_name.rindex('_')
            processed_name = temp_name[:last_underscore_index]
        temp_type = processed_name[0]

        temp_phase = processed_name[-1]
        processes_dict = {}
        #         break
        processes_dict['orig_name'] = temp_name
        processes_dict['index'] = index
        processes_dict['Multiplier'] = 1
        processes_dict['CIM phase'] = temp_phase
        processes_dict['CIM attribute'] = 'magnitude'
        processes_dict['CIM units'] = 'VA'
        processes_dict['CIM name'] = processed_name[2:-2].lower()  # Hope this is good :)
        #         print(temp_dict)
        #         break
        name_phase = processed_name
        if name_phase in conversion_name_dict:
            print('duplicate name', name_phase)
        else:
            conversion_name_dict[name_phase] = processes_dict
        if temp_type == 'V':
            processes_dict['CIM units'] = 'PNV'

        master_dict[rtu_string]['Analog input'][index] = processes_dict
    return master_dict, conversion_name_dict

def get_device_dict(model_dict, model_line_dict, device_type, name):
    if device_type == 'Shark':
        for meas in model_dict['feeders'][0]['measurements']:
            if meas['name'].startswith('ACLineSegment_'+name):
    #             print(meas)
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}
                if meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}
    elif device_type == 'RTU':
        for meas in model_dict['feeders'][0]['measurements']:
            if meas['name'].startswith('EnergyConsumer_'+name):
    #             print(meas)
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}
                if meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}

    elif device_type == 'Beckwith CapBank':
    #LinearShuntCompensator
        for meas in model_dict['feeders'][0]['measurements']:
            if meas['name'].startswith('LinearShuntCompensator_'+name):
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}
                elif meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}
                if meas['measurementType'] == 'Pos':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']]  = {'mrid':meas['mRID'],'type':'pos'}
                for cap in model_dict['feeders'][0]["capacitors"]:
                    if cap['name'] == name:
                        model_line_dict[name]['manual close'] = {'mrid':meas['mRID'],'type':'magnitude'}
                        print(cap)
                        #TODO figure this out
                        # 0 manual close
                        # 1 manual open
    elif device_type == 'Beckwith LTC':
        for meas in model_dict['feeders'][0]['measurements']:
            if meas['name'].startswith('RatioTapChanger_'+name):#ConductingEquipment_name
                if meas['measurementType'] == 'PNV':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}
                elif meas['measurementType'] == 'VA':
                    if meas['measurementType'] not in model_line_dict[name]:  model_line_dict[name][meas['measurementType']] ={}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'magnitude'}
                    model_line_dict[name][meas['measurementType']][meas['phases']] = {'mrid':meas['mRID'],'type':'angle'}
                if meas['measurementType'] == 'Pos':
                    if meas['measurementType'] not in model_line_dict[name]:
                        model_line_dict[name][meas['measurementType']] = {}
                    model_line_dict[name][meas['measurementType']][meas['phases']]  = {'mrid':meas['mRID'],'type':'pos'}
                for reg in model_dict['feeders'][0]["regulators"]:
                    if reg['bankName'] == name:
                        # Do I have to count for each position change?
                        print(reg)

def build_RTAC(filename_rtu=r'DNP3_Dict_Feb23(3).xlsx',
               filename_eq='DNP3 list.xlsx',
               sd_g_model=''):
    conversion_dict_master = {}
    conversion_name_dict_master = {}

    conversion_dict, conversion_name_dict = convert_rtu(csv_file=filename_rtu, sheet_name='RTU1')
    conversion_dict_master.update(conversion_dict)
    conversion_name_dict_master.update(conversion_name_dict)

    conversion_dict, conversion_name_dict = convert_rtu(csv_file=filename_rtu, sheet_name='RTU2')
    conversion_dict_master.update(conversion_dict)
    conversion_name_dict_master.update(conversion_name_dict)

    conversion_dict, conversion_name_dict = convert_rtu(csv_file=filename_rtu, sheet_name='RTU3')
    conversion_dict_master.update(conversion_dict)
    conversion_name_dict_master.update(conversion_name_dict)

    conversion_dict, conversion_name_dict = convert_rtu(csv_file=filename_rtu, sheet_name='RTU4')
    conversion_dict_master.update(conversion_dict)
    conversion_name_dict_master.update(conversion_name_dict)

    conversion_dict, conversion_name_dict = convert_rtu(csv_file=filename_rtu, sheet_name='RTU5')
    conversion_dict_master.update(conversion_dict)
    conversion_name_dict_master.update(conversion_name_dict)

    conversion_dict, conversion_name_dict = convert_rtu(csv_file=filename_rtu, sheet_name='RTU6')
    conversion_dict_master.update(conversion_dict)
    conversion_name_dict_master.update(conversion_name_dict)

    conversion_dict, conversion_name_dict = convert_rtu(csv_file=filename_rtu, sheet_name='RTU7')
    conversion_dict_master.update(conversion_dict)
    conversion_name_dict_master.update(conversion_name_dict)

    ## individual equipment conversion parts, saves to file
    build_eq_conversion_dict(filename_eq)

    # Load EQ model
    with open('conversion_dict_eq.json') as json_file:
        data = json.load(json_file)

    data.update(conversion_dict_master)

    with open("conversion_dict_master.json", "w") as f:
        json.dump(data, f, indent=2)
    conversion_dict = data

    with open(sd_g_model) as f:
        model_dict = json.load(f)

    model_line_dict = {}

    device_type = "Beckwith CapBank"
    name = "701-104cf"
    model_line_dict[name] = {}
    get_device_dict(model_dict, model_line_dict, device_type, name)

    device_type = 'Beckwith LTC'
    name = "s1"
    model_line_dict[name] = {}
    get_device_dict(model_dict, model_line_dict, device_type, name)

    for full_name, name_dict in conversion_name_dict_master.items():
        name = name_dict['CIM name']
        device_type = "RTU"
        #         name='tran_xf_701_1095507_5022'
        model_line_dict[name] = {}
        get_device_dict(model_dict, model_line_dict, device_type, name)

    with open("model_line_dict_master.json", "w") as f:
        json.dump(model_line_dict, f, indent=2)


def build_eq_conversion_dict(csv_file = 'DNP3 list.xlsx'):
    conversion_dict_eq = {}
    # csv_file = 'DNP3 list.xlsx'
    shark = get_conversion_model(csv_file, sheet_name='Shark')
    conversion_dict_eq['Shark'] = shark
    sheet_name = 'Beckwith CapBank 2'
    beckwith_capbank = get_conversion_model(csv_file, sheet_name)
    conversion_dict_eq['Beckwith CapBank'] = beckwith_capbank
    sheet_name = 'Beckwith LTC'
    beckwith_capbank = get_conversion_model(csv_file, sheet_name)
    conversion_dict_eq[sheet_name] = beckwith_capbank
    with open("conversion_dict_eq.json", "w") as f:
        json.dump(conversion_dict_eq, f, indent=2)


if __name__ == '__main__':
    build_RTAC(filename_rtu=r'DNP3_Dict_Feb23(3).xlsx',
               filename_eq='DNP3 list.xlsx',
               sd_g_model = '/Users/jsimpson/git/adms-use-case-3/GridAppsD_Usecase3/701_disag/701/221053913/model_dict.json')