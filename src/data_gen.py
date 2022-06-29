import pandas as pd
import zipfile as zf
import os
import re

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
    all_vars = table_vars + [table_pkey]
    df = df.loc[:, all_vars]
    if distinct_postings:
        df = df.drop_duplicates(subset = [table_pkey]).reset_index().drop("index", axis = 1)
    return df

def regex_subset(original, search_string, n_letters):
    regex_string = r'^' + re.escape(search_string[:n_letters]) + r'.*$'
    regex_subset = [m for m in original if re.search(regex_string, m)]
    return regex_subset

def unique_data_preparation(df, id, table, conn):
    # get existing records:
    existing_ids = conn.execute("SELECT DISTINCT {} FROM {}".format(id, table)).fetchall()
    existing_ids = [id[0] for id in existing_ids]
    # for every id check if it already exists and specify otherwise
    df = df.set_index(id)
    new_insertations = [name for name in df.index if name not in regex_subset(original_set = existing_ids, search_string = str(name), n_letters = 3)]
    df = df.loc[new_insertations, :].reset_index().dropna(subset=[id])
    return df

def test_data_structure(n_insertations, table, conn):
            n_row_table = conn.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
            assert n_insertations == n_row_table
