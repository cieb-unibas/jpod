import pandas as pd
import numpy as np
import zipfile as zf
import os

def select_raw_files(dir, file_format = ".zip"):
    """Return files of a given format from a directory."""
    files = os.listdir(dir)
    files = [file for file in files if file.endswith(file_format)]
    return files

def load_raw_data(file):
    """Read zipped or raw .csv file to a pandas DataFrame"""
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
    return df

def structure_data(df, table_vars, table_pkey, lowercase = None, distinct = True):
    """
    Structures and processes data before inserting it to JPOD.

    Keyword arguments:
    df -- A pandas DataFrame
    table_vars -- A list of strings that represent JPOD-columns the data will be inserted to.
    table_pkey -- A string representing the primary key in the JPOD-table data will be inserted to.
    lowercase -- A list of strings representing columns in the JPOD-table that have to be lowercase (default = None).
    distinct -- Should only observations with a non-duplicated primary key be considered (default = True)?

    Returns:
    A pandas DataFrame.
    """
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
    Subset data to observations with identifiers that are not present in JPOD yet.

    Keyword arguments:
    df -- A pandas DataFrame
    table -- A string indicating the JPOD-table the data will be inserted to.
    id -- A string representing the primary key of the JPOD-table
    conn -- Connection to JPOD (a sqlite3 object)

    Returns:
    A pandas DataFrame.
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

def insert_base_data(df, table, conn):
    """
    Insert data into JPOD.

    Keyword arguments:
    df -- A pandas DataFrame
    table -- A string indicating the JPOD-table the data will be inserted to.
    conn -- Connection to JPOD (a sqlite3 object)
    """
    if len(df) == 0:
        print("No data to insert into JPOD table '{}'.".format(table))
    else:
        projected_insertations = len(df)
        rows_pre = conn.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
        df.to_sql(name = table, con = conn, index = False, if_exists = "append")
        rows_post = conn.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
        assert rows_post - rows_pre == projected_insertations, "Number of rows in the original dataframe does not correspond to the number of inserted rows to the database table"
        print("Data successfully inserted into JPOD table '{}'.".format(table))


# tbd for updates: create new uniq_ids
# def create_id(chars = string.ascii_lowercase + string.digits):
#     id = ''.join(random.choice(chars) for x in range(32))
#     return id

# def create_identifiers(n_ids, conn, table, id)
#     existing_ids = conn.execute("SELECT {} FROM {}".format(id, table)).fetchall()
#     existing_ids = np.array(existing_ids, dtype=str)
        
    






