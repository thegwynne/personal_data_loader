from google.cloud import bigquery
import requests
import pandas as pd
from google.cloud import secretmanager
import json
import datetime
import logging
import os

project_id = "cloudhal9000"
dataset_name = "lifestyle"
base_url = "https://api.octopus.energy/v1/"

def get_secrets():
    env_secrets = os.getenv('api_details')
    if env_secrets:
        print(f"secrets length: {len(env_secrets)}")
        return json.loads(env_secrets)
    secret_id = "octopus_api"
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(secret_path)
    
    return json.loads(response.payload.data.decode('utf-8'))
    

def api_request(url, secrets):
    rows = []
    next_request_url = url
    while True:
        session = requests.Session()
        session.auth = (secrets.get("api_key"),'')
        response = session.get(next_request_url)
        json_response = json.loads(response.content)
        rows += json_response.get('results')
        if not json_response.get('next'):
            break
        next_request_url = json_response.get('next')
    return rows

def write_to_bq(client, df, project_dataset_table, write_disposition = 'WRITE_APPEND'):
    
    schema = [bigquery.SchemaField("consumption", "float64"),
                bigquery.SchemaField("interval_start", "datetime"),
                bigquery.SchemaField("interval_end", "datetime"),
                bigquery.SchemaField("inserted", "datetime")]
    for col in ["interval_start", "interval_end", "inserted"]:
        df[col] = pd.to_datetime(df[col])
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition,schema=schema)
    client.load_table_from_dataframe(df, project_dataset_table,job_config=job_config)
    return

def get_table_latest_ts(client):
    query = f"""select format_datetime("%Y-%m-%dT%H:%MZ", max(interval_end)) as ts 
                from `{project_id}.{dataset_name}.electricity_consumption`"""
    df = client.query(query).to_dataframe()
    if len(df) == 1:
        return df['ts'].iloc[0]
    else:
        logging.error("Error getting latest timestamp from bq")
        return None
    
def run(secrets,client,datetimeto, datetimefrom, request_timestamp,reset=False):
    parameters = f"?page_size=100&period_from={datetimefrom}&period_to={datetimeto}&order_by=period"

    electric_usage_url = f"{base_url}electricity-meter-points/{secrets.get('electric_MPAN')}/meters/{secrets.get('electric_serial_no')}/consumption/"
    gas_usage_url = f"{base_url}gas-meter-points/{secrets.get('gas_MPRN')}/meters/{secrets.get('gas_serial_no')}/consumption/"

    electric_rows = api_request(electric_usage_url + parameters, secrets)
    electric_df = pd.DataFrame.from_records(electric_rows)
    electric_df['inserted'] = request_timestamp
    if reset:
        write_to_bq(client, electric_df, f'{project_id}.{dataset_name}.electricity_consumption',write_disposition='WRITE_TRUNCATE')
    else:
        write_to_bq(client, electric_df, f'{project_id}.{dataset_name}.electricity_consumption')

    gas_rows = api_request(gas_usage_url + parameters, secrets)
    gas_df = pd.DataFrame.from_records(gas_rows)
    gas_df['inserted'] = request_timestamp
    if reset:
        write_to_bq(client, gas_df,f'{project_id}.{dataset_name}.gas_consumption',write_disposition='WRITE_TRUNCATE' )
    else:
        write_to_bq(client, gas_df,f'{project_id}.{dataset_name}.gas_consumption')
    return 

def catchup():
    secrets = get_secrets()
    client = bigquery.Client(project=project_id)

    datetime_now = datetime.datetime.utcnow()
    request_timestamp = datetime_now.isoformat()
    datetime_now = (datetime_now - datetime.timedelta(minutes=datetime_now.minute % 30))
    datetimeto = datetime_now.strftime("%Y-%m-%dT%H:%MZ")
    datetimefrom = get_table_latest_ts(client)
    run(secrets, client, datetimeto, datetimefrom, request_timestamp)
    return


def reset(days_to_catchup):
    secrets = get_secrets()
    client = bigquery.Client(project=project_id)

    datetime_now = datetime.datetime.utcnow()
    request_timestamp = datetime_now.isoformat()
    datetime_now = (datetime_now - datetime.timedelta(minutes=datetime_now.minute % 30))
    datetimeto = datetime_now.strftime("%Y-%m-%dT%H:%MZ")
    datetimefrom = (datetime_now - datetime.timedelta(days=days_to_catchup)).strftime("%Y-%m-%dT%H:%MZ")
    run(secrets, client, datetimeto, datetimefrom, request_timestamp,reset=True)
    return

def main(request):
    reset_days = request.args.get('reset')
    if reset_days:
        reset(int(reset_days))
    else:
        catchup()
    return 'Done'