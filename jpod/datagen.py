import pandas as pd
import zipfile as zf
import os
import re

def select_raw_files(dir, file_format = ".zip"):
    files = os.listdir(dir)
    files = [file for file in files if file.endswith(file_format)]
    return files

def load_raw_data(file):
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
    df = df.rename(columns = {"inferred_iso3_lang_code": "text_language", "is_remote": "remote_position"})
    return df

def structure_data(df, table_vars, table_pkey, lowercase_vars = None, distinct_postings = True):
    table_vars = list(set(table_vars + [table_pkey]))
    df = df.loc[:, table_vars]
    if distinct_postings:
        df = df.drop_duplicates(subset = [table_pkey]).reset_index().drop("index", axis = 1)
    if lowercase_vars is not None:
        lowercase_vars = [v for v in lowercase_vars if v in table_vars]
        for v in lowercase_vars:
            df[v] = df[v].astype(str).str.strip().str.lower()
    return df

def regex_subset(full_set, search_string, n_letters):
    regex_string = r'^' + re.escape(search_string[:n_letters]) + r'.*$'
    regex_subset = [m for m in full_set if re.search(regex_string, m)]
    return regex_subset

def unique_data_preparation(df, id, table, conn):
    # get existing records:
    existing_ids = conn.execute("SELECT DISTINCT {} FROM {}".format(id, table)).fetchall()
    existing_ids = [str(id[0]).strip().lower() for id in existing_ids]
    # for every id check if there is a match with existing ones. keep otherwise
    df = df.set_index(id)
    new_insertations = [name for name in df.index if name not in regex_subset(full_set = existing_ids, search_string = str(name), n_letters = 3)]
    df = df.loc[new_insertations, :].reset_index().drop_duplicates(subset = [id]).dropna(subset=[id])
    return df

def test_data_structure(n_insertations, table, conn):
            n_row_table = conn.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
            assert n_insertations == n_row_table, "Number of rows in the original dataframe does not correspond to the number of inserted rows to the database table"

def insert_base_data(table_dat, table, conn):
    projected_insertations = len(table_dat)
    table_dat.to_sql(name = table, con = conn, index = False, if_exists = "append")
    test_data_structure(n_insertations = projected_insertations, table = table, conn = conn)
    print("Data successfully inserted into JPOD table '{}'.".format(table))
