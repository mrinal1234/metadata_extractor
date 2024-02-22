import os
import glob
import pprint
import re
import typing
from itertools import cycle
import pandas as pd
from sql_metadata import Parser
import time
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', 2000)


ROOT_DIR = os.path.abspath("")
INPUT_DIR = os.path.join(ROOT_DIR,'input')
OUTPUT_DIR = os.path.join(ROOT_DIR,'output')

def split_table_info(table_info: str):
    """
    Split schema.table into schema and table
    """
    table_info = table_info.split(".")
    #print('table_info', table_info)
    if len(table_info) >= 3:
        table_name = table_info[-1]
        table_schema_name = table_info[-2]
    elif len(table_info) == 2:
        table_name = table_info[1]
        table_schema_name = table_info[0]
    else:
        table_name = table_info[0]
        table_schema_name = ''
    
    return table_schema_name, table_name

def create_view_name(block : str):
    """
    Extract schema and view name from SQL query
    """
    metadata = []
    for s in block.split('\n'):
        if re.search("^CREATE OR REPLACE",s):
            schema_proc = s.strip().split(' ')[-1]
            #print('schema_proc', schema_proc)
            Schema, ViewName = split_table_info(schema_proc)
            metadata.append([Schema, ViewName])
        else:
            pass
        
    return metadata


if __name__ == "__main__":
    
    os.chdir(INPUT_DIR)
    df = pd.DataFrame(columns = ['File', 'Schema', 'View_Name',
                             'Operation', 'Table_Schema_Name','Tables', 'Sequence'])
    
    for filename in glob.glob('*.sql'):
        print('***** Initiating metadata extraction *********************\n')
        print(f'### Input directory: {INPUT_DIR}')
        fname = filename.replace(".sql","")
        print(f'### Filename: {fname}\n')
        file = open(filename, 'r')
        sqlFile = file.read()
        file.close()
        lines = re.split(";\n\n", sqlFile)
        print(f'### No of Views:{len(lines)}\n')
        
        print('--- Starting metadata extraction ...\n')
        
        d1 = []
        for line in lines:
            m = create_view_name(line)
            if len(m) != 0:
                block = re.split('AS\s+\n', line.strip(), 0)[-1]
                if block.startswith("SELECT"):
                    table_list = Parser(block).tables
                    mylist = []
                    for table in table_list:
                        table_schema_name, table_name = split_table_info(table)
                        mylist.append(['Select', table_schema_name, table_name])

                result = [[m[0][0]]+[m[0][1]]+i for i in mylist]
            else:
                break
            d1.extend(result)
            print(f'... Extraction complete for : {m[0][1]}')

        # Convert d1 into dataframe 
        print('\n----- Converting extracted data into .csv file ...\n')
        d2 = pd.DataFrame(d1, columns = ['Schema', 'View_Name', 'Operation', 'Table_Schema_Name','Tables'])
        d2.insert(0, 'File', filename)
        d2 = d2[~(d2.Tables.isin(["SET", "UPDATE", "(SELECT", "_DT,"]))]
        d2['Sequence'] = d2.reset_index().index+1
        df = pd.concat([d2])

        df.to_csv(OUTPUT_DIR+'/'+fname+".csv", index=None)
    
    print('***** Metadata extraction complete ****************')