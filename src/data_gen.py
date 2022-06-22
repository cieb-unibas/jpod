import pandas as pd
import zipfile as zf
import os

def select_files(dir, file_format):
    files = os.listdir(dir)
    files = [file for file in files if file.endswith(file_format)]
    return files

def load_data(file):
    if file.endswith(".zip"):
        file = zf.ZipFile(file)
        file_list = file.infolist()
        assert len(file_list) == 1
        file_name = file_list[0].filename
        df = pd.read_csv(file.open(file_name))
    elif file.endswith(".csv"):
        df = pd.read_csv(file)
    else:
        raise ValueError("Data must be provided in raw or zipped .csv format.")
    return df

def structure_data(df, table_vars, table_pkey, distinct_postings = True):
    df = df.loc[:, table_vars]
    if distinct_postings:
        df = df.drop_duplicates(subset = [table_pkey])
    return df