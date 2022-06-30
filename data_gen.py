import pandas as pd
import zipfile as zf
import os
import re
import sqlite3

class jpod_properties():
    def __init__(self):
        self.tables = ["job_postings", "position_characteristics", "institutions"]
        self.pkeys = {"job_postings": "uniq_id", "position_characteristics": "uniq_id", "institutions": "company_name"}
        self.tablevars = {
            "job_postings": ["crawl_timestamp", "url", "post_date", "job_title", "job_description",
            "html_job_description", "job_board", "text_language"
            ],
            "position_characteristics": ["company_name", "category", "inferred_department_name", 
            "inferred_department_score", "city", "inferred_city", "state", "inferred_state", 
            "country", "inferred_country", "job_type", "inferred_job_title", "remote_position"
            ],
            "institutions": ["contact_phone_number", "contact_email", "inferred_company_type", "inferred_company_type_score"
            ]}
        self.str_matching_vars = [
            "job_title", "job_board", "company_name", "category", "inferred_department_name",
            "city", "inferred_city", "state", "inferred_state", "country", "inferred_country", 
            "job_type", "inferred_job_title", "inferred_company_type"]
    
def jpod_connect(db_path):
        conn = sqlite3.connect(db_path + "jpod.db")
        return conn

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

def structure_data(df, table_vars, table_pkey, lower_string_vars = None, distinct_postings = True):
    all_vars = table_vars + [table_pkey]
    df = df.loc[:, all_vars]
    if distinct_postings:
        df = df.drop_duplicates(subset = [table_pkey]).reset_index().drop("index", axis = 1)
    if lower_string_vars is not None:
        for v in lower_string_vars:
            df[v] = [str(x).strip().lower() for x in df[v]]
    return df

def regex_subset(full_set, search_string, n_letters):
    regex_string = r'^' + re.escape(search_string[:n_letters]) + r'.*$'
    regex_subset = [m for m in full_set if re.search(regex_string, m)]
    return regex_subset

def unique_data_preparation(df, id, table, conn):
    # get existing records:
    existing_ids = conn.execute("SELECT DISTINCT {} FROM {}".format(id, table)).fetchall()
    existing_ids = [str(id[0]).strip().lower() for id in existing_ids]
    # for every id check if it already exists and specify otherwise
    # df[id] = [str(name).strip().lower() for name in df[id]] => not necesseray anymore
    df = df.set_index(id)
    new_insertations = [name for name in df.index if name not in regex_subset(full_set = existing_ids, search_string = str(name), n_letters = 3)]
    df = df.loc[new_insertations, :].reset_index().drop_duplicates(subset = [id]).dropna(subset=[id])
    return df

def test_data_structure(n_insertations, table, conn):
            n_row_table = conn.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
            assert n_insertations == n_row_table
