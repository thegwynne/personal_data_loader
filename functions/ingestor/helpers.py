from datetime import datetime
import pandas as pd

def proc_datatypes(dataframe, schema):
#Handle integer columns - convert to type
    intcolumns = [col.name for col in schema if col.field_type == 'INTEGER']
    for col in intcolumns:
        dataframe[col] = dataframe[col].astype('int64', errors='ignore')
#Handle float columns - remove readability aids (commas, parentheses)
    floatcolumns = [col.name for col in schema if col.field_type == 'FLOAT']
    for col in floatcolumns:
        if dataframe[col].dtype not in ['float64', 'int64']:
            dataframe[col] = dataframe[col].str.replace(',','').str.replace('(','').str.replace(')','')
        dataframe[col] = dataframe[col].astype(float)
#Handle date columns - convert to type, and remove empty values
    datecolumns = [col.name for col in schema if col.field_type == 'DATE' or col.field_type == 'DATETIME']
    for col in datecolumns:
        dataframe[col] = dataframe[col].replace(0.0, pd.NA)
        dataframe[col] = pd.to_datetime(dataframe[col])
#Handle string columns - ensure not interpretted as numbers
    stringcolumns = [col.name for col in schema if col.field_type == 'STRING']
    for col in stringcolumns:
        if str(dataframe[col].dtype) in ['int64', 'float64']:
            dataframe[col] = dataframe[col].fillna('').astype(str)
        
    return dataframe

def string_clean(inputstr: str):
    return inputstr.replace(' - ', '__').replace(' ', '_')\
                  .replace('%', 'PCT').replace('-','_')\
                  .replace('+','PLUS').replace('(','_')\
                  .replace(')','_').replace(':','__')\
                  .replace(',','__').replace('/','_')\
                  .replace('.', '')



def augment_dataframe(dataframe, filename):
    dataframe['Filename'] = filename
    dataframe['Inserted_Timestamp'] = datetime.utcnow().isoformat()
    dataframe['Inserted_Timestamp'] = pd.to_datetime(dataframe['Inserted_Timestamp'])
    return dataframe