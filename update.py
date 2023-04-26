import sqlite3
import pandas as pd

import jpod

JPOD_VERSION = "jpod_test.db"
JPOD_CONN = sqlite3.connect("C:/Users/matth/Desktop/" + JPOD_VERSION)
JPOD_STRUCTURE = jpod.base_properties()

DAT_DIR = "C:/Users/matth/Documents/github_repos/"
FILES = jpod.select_raw_files(DAT_DIR)

def get_jpod_columns(conn = JPOD_CONN):
    jpod_tables = [t for t in jpod.get_tables(conn) if t != "regio_grid"]
    jpod_columns = [v[1] for t in jpod_tables for v in conn.execute("PRAGMA table_info(%s);" % t).fetchall()]
    return jpod_columns

def load_and_structure(file = DAT_DIR + FILES[0], conn = JPOD_CONN):
    df = jpod.load_raw_data(file)
    df = df.rename(columns = {"inferred_iso3_lang_code": "text_language", "is_remote": "remote_position"})
    cols = get_jpod_columns(conn)
    df = df[[c for c in df.columns if c in cols]]
    return df

def lowercase_columns(dat, columns):
    for c in columns:
        c_notnull = dat[c].notnull()
        c_lower = dat[c].dropna().astype(str).str.strip().str.lower()
        dat.loc[c_notnull, c] = c_lower
    return dat

def load_jpod_nuts(conn):
    # nuts regions and codes
    nuts_query = """
    SELECT name_en AS inferred_state, nuts_2, nuts_3
    FROM regio_grid
    WHERE nuts_level = 2 OR nuts_level = 3;
    """
    regio_nuts = pd.read_sql(con = conn, sql = nuts_query)
    # oecd regions and codes
    oecd_query = """
    SELECT name_en AS inferred_state, oecd_tl2 AS nuts_2, oecd_tl3 AS nuts_3
    FROM regio_grid
    WHERE nuts_2 IS NULL AND nuts_3 IS NULL  AND (oecd_level = 2 OR oecd_level = 3);
    """
    regio_oecd = pd.read_sql(con = conn, sql = oecd_query)
    # combine and return
    regions = pd.concat([regio_nuts, regio_oecd], axis= 0)
    return regions



#### load data
df = load_and_structure()
df = lowercase_columns(df, columns = JPOD_STRUCTURE.lowercase_vars)

#### assign regional codes
df = df.merge(load_jpod_nuts(conn=JPOD_CONN), how="left", on = "inferred_state")

# indicate duplicate status

# identify bloom

# identify ai

# insert to jpod

if __name__ == "__main__":
    print("---------------Updating JPOD---------------")
    DAT_DIR = "C:/Users/matth/Documents/github_repos/"
    FILES = jpod.select_raw_files(DAT_DIR)
    log_n = 20
    for file, i in enumerate(FILES):
        # load and structure data
        df = load_and_structure(file = DAT_DIR + file, conn=JPOD_CONN)
        df = lowercase_columns(df, columns = JPOD_STRUCTURE.lowercase_vars)
        # assign regional codes to samples
        df = df.merge(load_jpod_nuts(conn=JPOD_CONN), how="left", on = "inferred_state")
        # indicate duplicate status

        # logs
        if i % log_n == 0:
            print("Inserted data from %d file into JPOD" % i)

