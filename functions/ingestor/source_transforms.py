from helpers import proc_datatypes

def transform_streaming_history_extended(dataframe, schema):
    dataframe["offline_timestamp"] = dataframe["offline_timestamp"].replace(0, None)
    dataframe = proc_datatypes(dataframe, schema)
    return dataframe

def transform_word_stresses(dataframe, schema):
    dataframe = proc_datatypes(dataframe, schema)
    return dataframe

def transform_song_list(dataframe, schema):
    dataframe["Date_Added"] = dataframe["Date_Added"].replace('0000-00-00', None)
    dataframe = proc_datatypes(dataframe, schema)
    return dataframe

transform_switcher = {
    "streaming_history_extended": transform_streaming_history_extended,
    "word_stresses": transform_word_stresses,
    "song_list": transform_song_list
}
