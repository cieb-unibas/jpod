import pandas as pd
import numpy as np
import zipfile as zf
import os

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
        df = pd.read_csv(file.open(file_name), low_memory=False)
    elif file.endswith(".csv"):
        df = pd.read_csv(file)
    else:
        raise ValueError("Data must be provided in raw or zipped .csv format.")
    df = df.rename(columns = {"inferred_iso3_lang_code": "text_language", "is_remote": "remote_position"})
    return df

def structure_data(df, table_vars, table_pkey, lowercase = None, distinct = True):
    table_vars = list(set(table_vars + [table_pkey]))
    df = df.loc[:, table_vars]
    if lowercase is not None:
        lowercase_vars = [v for v in lowercase if v in table_vars]
        for v in lowercase_vars:
            df[v] = df[v].astype(str).str.strip().str.lower()
    if distinct:
        df = df.drop_duplicates(subset = [table_pkey]).reset_index().drop("index", axis = 1)
    return df

def unique_records(df, id, table, conn):
    """
    Ensures that only records with a non-existing identifier in the database are inserted into JPOD.
    """
    existing_ids = conn.execute("SELECT {} FROM {}".format(id, table)).fetchall()
    existing_ids = np.array(existing_ids, dtype=str)
    n_existing = len(existing_ids)
    if n_existing > 0:
        existing_ids = np.char.strip(existing_ids)
        existing_ids = np.char.lower(existing_ids)
        new_insertations = np.setdiff1d(list(df[id]), list(existing_ids))
        df = df.set_index(id)
        df = df.loc[new_insertations, :].reset_index().drop_duplicates(subset = [id]).dropna(subset=[id])
    return df

def insert_base_data(table_dat, table, conn):
    """
    Inserts data from a pd.DataFrame into JPOD.
    """
    if len(table_dat) == 0:
        print("No data to insert into JPOD table '{}'.".format(table))
    else:
        projected_insertations = len(table_dat)
        rows_pre = conn.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
        table_dat.to_sql(name = table, con = conn, index = False, if_exists = "append")
        rows_post = conn.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
        assert rows_post - rows_pre == projected_insertations, "Number of rows in the original dataframe does not correspond to the number of inserted rows to the database table"
        print("Data successfully inserted into JPOD table '{}'.".format(table))