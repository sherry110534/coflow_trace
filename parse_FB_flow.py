import json
import random

# MAPPER NUM 150 (id:0~149)
# REDUCER NUM 150 (id:0~149)

INPUTFILE = 'FB2010-1Hr-150-0.txt'
OUTPUTFILE = 'coflow_data.json'
def read_data():
    data_line = []
    with open(INPUTFILE, 'r') as f:
        for line in f:
            data_line.append(line.replace('\n', '').split(' '))
    data_line = data_line[1:]
    return data_line

def parse(data_line):
    tmp_dict = {}
    tmp_dict['Coflow ID'] = int(data_line[0])
    tmp_dict['Arrival time'] = int(data_line[1])
    tmp_dict['Mapper num'] = int(data_line[2])
    tmp_dict['Mapper list'] = []
    for i in range(tmp_dict['Mapper num']):
        tmp_dict['Mapper list'].append(int(data_line[3+i]))
    tmp_dict['Reducer num'] = int(data_line[3+tmp_dict['Mapper num']])
    tmp_dict['Reducer list'] = []
    tmp_dict['Reducer data'] = []
    for i in range(tmp_dict['Reducer num']):
        tmp_dict['Reducer list'].append(data_line[3+tmp_dict['Mapper num']+1+i])
    for i in range(tmp_dict['Reducer num']):
        tmp_s = tmp_dict['Reducer list'][i].split(":")
        tmp_dict['Reducer list'][i] = int(tmp_s[0])
        tmp_dict['Reducer data'].append(float(tmp_s[1]))
    tmp_dict["Packet mean"] = random.randint(1024, 8192)
    tmp_dict["Packet scale"] = random.randint(128, 512)
    return tmp_dict

if __name__ == '__main__':
    output_list = []
    data = read_data()
    for d in data:
        output_list.append(parse(d))  
    with open(OUTPUTFILE, "w") as f:
        json.dump(output_list, f)
    print "complete"

    