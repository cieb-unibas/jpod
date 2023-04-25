import sqlite3

import jpod

JPOD_VERSION = "jpod_test.db"
JPOD_CONN = sqlite3.connect("C:/Users/matth/Desktop/" + JPOD_VERSION)
conn = JPOD_CONN

DAT_DIR = "C:/Users/matth/Documents/github_repos/"
FILES = jpod.select_raw_files(DAT_DIR)

def get_jpod_columns(conn):
    jpod_tables = [t for t in jpod.get_tables(conn) if t != "regio_grid"]
    jpod_columns = [v[1] for t in jpod_tables for v in conn.execute("PRAGMA table_info(%s);" % t).fetchall()]
    return jpod_columns

def load_and_structure(file = DAT_DIR + FILES[0], conn = JPOD_CONN):
    df = jpod.load_raw_data(file)
    df = df.rename(columns = {"inferred_iso3_lang_code": "text_language", "is_remote": "remote_position"})
    cols = get_jpod_columns(conn)
    df = df[[c for c in df.columns if c in cols]]
    return df

# assign geo

# clean duplicates

# assign bloom

# assign ai

