import pandas as pd

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

load_switcher = {
    "streaming_history_extended": load_streaming_history_extended,
    "word_stresses": load_word_stresses,
    "song_list": load_song_list
}
