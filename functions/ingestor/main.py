from google.cloud import storage, bigquery
import pandas as pd
from datetime import datetime
import json
from helpers import string_clean, augment_dataframe

from source_loaders import load_switcher
from source_transforms import transform_switcher
from source_datasets import dataset_switcher

gcs_client = storage.Client()
bq_client = bigquery.Client()


project_id = "cloudhal9000"

landing_zone_bucket_name = "cloudhal9000-landing-zone"
schema_bucket_name = "cloudhal9000-schemas"
archive_bucket_name = "cloudhal9000-archive"
error_bucket_name = "cloudhal9000-error"


def get_schema(table_name):
    schema_bucket = gcs_client.get_bucket(schema_bucket_name)
    schema_blob = schema_bucket.get_blob(table_name + '.schema')
    raw_str = schema_blob.download_as_string()
    json_schema = json.loads(raw_str)
    schema = proc_json_schema(json_schema)

    return schema

def proc_json_schema(json_schema):
    """
    Converts bigquery table schema from .json file as stored in gcs -schema bucket into a list of 
    bigquery.SchemaField instances, with nested records handled appropriately.
    """
    result = []
    for field in json_schema:
        if field['type'].upper() == "RECORD":
            sub_result = proc_json_schema(field['fields'])
            result.append(bigquery.SchemaField(field["name"], field["type"], mode=field["mode"], fields=sub_result))
        else:
            result.append(bigquery.SchemaField(field["name"], field["type"], mode = field.get("mode", "NULLABLE"))) 
    return result


def move_gcs_file(sourcebucket_name, targetbucket_name, filename):
    sourcebucket = gcs_client.get_bucket(sourcebucket_name)
    targetbucket = gcs_client.get_bucket(targetbucket_name)
    source_blob = sourcebucket.blob(filename)
    new_filename = filename + '.' + datetime.utcnow().isoformat()
    new_blob = sourcebucket.copy_blob(source_blob, targetbucket, new_filename)
    source_blob.delete()
    return

def main(event, context = None):
    """
    Sequence:
        -Identify target table for file
        -Download contents of file to pandas dataframe, using load function defined in source_loaders.py
        -Get schema file from gcs
        -Augment dataframe with filename, insertion timestamp
        -Perform custom processing tasks using transform function defined in source_transforms.py
        -Load data to bigquery
        -Move file to archive bucket
    """

    file_name = event.get("name")
    table_name = file_name.split('/')[0]
    dataset_name = dataset_switcher.get(table_name)
    project_dataset_table = project_id + '.' + dataset_name + '.' + table_name
    just_file = file_name.split('/')[1]
    try:
        load_function = load_switcher.get(table_name)
        df = load_function('gs://' + landing_zone_bucket_name + '/' + file_name)
    except Exception as e:
        print("Error loading file into dataframe")
        print(e)
        move_gcs_file(landing_zone_bucket_name, error_bucket_name, file_name)
        return
    df.columns = [string_clean(c) for c in df.columns]
    try:
        schema = get_schema(table_name)
    except Exception as e:
        print("Error acquiring schema")
        print(e)
        move_gcs_file(landing_zone_bucket_name, error_bucket_name, file_name)
        return
    
    try:
        df = augment_dataframe(df, just_file)
        proc_function = transform_switcher.get(table_name)
        df = proc_function(df, schema)
    except Exception as e:
        print("Error processing dataframe")
        print(e)
        move_gcs_file(landing_zone_bucket_name, error_bucket_name, file_name)
        return
    try:
        job_config = bigquery.LoadJobConfig(     write_disposition="WRITE_APPEND",        create_disposition="CREATE_IF_NEEDED",       schema=schema  )
        job = bq_client.load_table_from_dataframe(df,project_dataset_table,job_config=job_config)
        job.result()
    except Exception as e:
        print("Error writing to bq")
        print(e)
        move_gcs_file(landing_zone_bucket_name, error_bucket_name, file_name)
        return
    print("Data written to bq")
    move_gcs_file(landing_zone_bucket_name, archive_bucket_name, file_name)
    return



# #########################################################################################
# def findarrowerrors(table_name, dataframe, schema):
#     successes = []
#     fails = []
#     for i in range(len(dataframe.columns)):
#         minidf = pd.DataFrame(dataframe[dataframe.columns[i]])
#         minischema = [schema[i]]
#         job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND",create_disposition="CREATE_IF_NEEDED",schema=minischema)
#         try:
#             job = bq_client.load_table_from_dataframe(minidf,project_id + '.' + dataset_name + '.' + table_name,job_config=job_config)
#             job.result()
#             successes.append(schema[i].name)
#         except:
#             fails.append(schema[i].name)
#     return successes, fails
