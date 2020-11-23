import sys
sys.path.append("../dnp3/service")

from dnp3.dnp3_to_cim import build_conversion

'''
Build the conversion dictionary from the DNP3 list.xlsx
'''
if __name__ == '__main__':
    csv_file = "DNP3 list.xlsx"
    build_conversion(csv_file=csv_file)