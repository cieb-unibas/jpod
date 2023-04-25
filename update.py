import sqlite3
import pandas as pd

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

df = load_and_structure()

#### REGIO ASSIGNMENT

# assign nuts-3 & oecd-3
regio_grid = pd.read_sql(con=JPOD_CONN, sql="SELECT * FROM regio_grid;")
countries = list(set(df["inferred_country"]))

regions = list(set(df["inferred_state"]))
regions = [r.lower() for r in regions]
region_to_nuts = {r: nuts3 for r in regio_grid.loc[regio_grid.nuts_level == 3, "name_en"]}


# assign nuts-3 & oecd-3

df.columns
tmp = df.loc[df.inferred_country == "United kingdom"]
tmp[['city', 'state', 'country', 'inferred_city', 'inferred_state','inferred_country']]

# clean duplicates

# assign bloom

# assign ai

