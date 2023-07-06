import sys
import os
import sqlite3

import pandas as pd

print("Current directory is: " + os.getcwd())
sys.path.append(os.getcwd())
import jpod

#### connect to JPOD
JPOD_VERSION = "jpod.db"
DB_DIR = os.path.join(jpod.get_path(jpod.config.DB_DIRS), JPOD_VERSION)
JPOD_CONN = sqlite3.connect(database = DB_DIR)

#### load data
df = pd.read_csv("data/regio_grid.csv", encoding= "latin-1", sep = ";")
df[["name_en", "name_de", "name_fr"]] = df[["name_en", "name_de", "name_fr"]].apply(lambda x: x.str.lower())

#### create regio table in JPOD 
JPOD_QUERY = """
DROP TABLE IF EXISTS regio_grid;
CREATE TABLE regio_grid (
name_en TEXT NOT NULL,
name_de TEXT,
name_fr TEXT,
regio_abbrev VARCHAR(5),
nuts_level INT,
oecd_level INT,
iso_2 VARCHAR(2),
iso_3 VARCHAR(3),
nuts_0 VARCHAR(2),
nuts_1 VARCHAR(3),
nuts_2 VARCHAR(4),
nuts_3 VARCHAR(5),
oecd_tl1 VARCHAR(2),
oecd_tl2 VARCHAR(4),
oecd_tl3 VARCHAR(5),
self_classified VARCHAR(3),
PRIMARY KEY (name_en, nuts_3)
);
"""
JPOD_CONN.executescript(JPOD_QUERY)
JPOD_CONN.commit()
assert "regio_grid" in jpod.get_tables(conn = JPOD_CONN)
assert all([col in jpod.get_table_vars(conn = JPOD_CONN, table = "regio_grid") for col in df.columns])
df.to_sql("regio_grid", con = JPOD_CONN, if_exists="append", index=False)

#### Save and close connection to .db -------------------
JPOD_CONN.commit()
JPOD_CONN.close()
