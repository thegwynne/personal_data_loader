import pandas as pd
from google.cloud import storage
import datetime
from io import StringIO
import re

def load_streaming_history_extended(gcspath):
    df = pd.read_json(gcspath, encoding_errors="ignore")
    return df

def load_word_stresses(gcspath):
    df = pd.read_csv(gcspath, sep = '\t', header=None)
    df.set_axis(["pronounced", "stress_pattern", "syllable_count", "word", "word_frequency", "word_type"], axis="columns", inplace=True)
    return df

def load_song_list(gcspath):
    df = pd.read_csv(gcspath, sep = ';')    
    return df

def load_whatsapp_chat__process_row(input_str):
    message_timestamp = datetime.datetime.strptime(input_str.split(' - ')[0], '%d/%m/%Y, %H:%M')
    message_sender = input_str.split(' - ')[1].split(':')[0]
    message_contents = ':'.join(input_str.split(': ')[1:])
    message_contents = message_contents.replace('"', '\'')
    return_str = "\"" + "\",\"".join([message_timestamp.isoformat(), message_sender, message_contents]) + "\""
    return return_str


def load_whatsapp_chat(gcspath):
    gcs_client = storage.Client()
    bucketname = gcspath.split('/')[2]
    filename = '/'.join(gcspath.split('/')[3:])
    bucket = gcs_client.get_bucket(bucketname)
    blob = bucket.get_blob(filename)
    raw_str = blob.download_as_string().decode()
    rows = raw_str.split('\n')
    cleaned_str = 'timestamp,sender,message'
    cleaned_rows = []
    counter = 0
    while True:
        active_row = rows[counter]
        # Handle end of file
        if counter + 1 == len(rows):
            print("End of file reached")
            if active_row != '':
                cleaned_str += '\n' + load_whatsapp_chat__process_row(active_row)
                cleaned_rows.append(load_whatsapp_chat__process_row(active_row))
            break
        # Handle multirow messages
        offset = 1
        while True:
            if counter + offset + 1 == len(rows):
                break
            if not re.match(r'[0-9]{2}/[0-9]{2}/[0-9]{4}', rows[counter + offset][:10]):
                active_row += ' ' + rows[counter + offset]
                offset += 1
            else:
                break
        cleaned_str += '\n' + load_whatsapp_chat__process_row(active_row)
        cleaned_rows.append(load_whatsapp_chat__process_row(active_row))
        counter += offset
    csvStringIO =StringIO(cleaned_str)
    df = pd.read_csv(csvStringIO)
    return df

def load_strava_activities(gcspath):
    df = pd.read_csv(gcspath)
    valid_columns = [col for col in df.columns if '<' not in col]
    df = df[valid_columns]
    df = df.rename(columns={"Filename": "Raw_Filename"})
    return df


load_switcher = {
    "streaming_history_extended": load_streaming_history_extended,
    "word_stresses": load_word_stresses,
    "song_list": load_song_list,
    "whatsapp_chat": load_whatsapp_chat,
    "strava_activities": load_strava_activities
}
