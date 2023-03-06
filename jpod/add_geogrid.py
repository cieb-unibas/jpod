import sys
import os
import sqlite3

import navigate as nav
import pandas as pd

#### connect to JPOD
DB_DIR = sys.argv[1]
DB_VERSION = sys.argv[2]
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)

#### load data
HOME = os.path.abspath(os.path.join(__file__, os.pardir, os.pardir))
os.chdir(HOME)
df = pd.read_csv("data/regio_grid.csv", sep = ";")
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
PRIMARY KEY (name_en, nuts_3)
);
"""
JPOD_CONN.executescript(JPOD_QUERY)
JPOD_CONN.commit()
assert "regio_grid" in nav.get_tables(conn = JPOD_CONN)
assert all([col in nav.get_table_vars(conn = JPOD_CONN, table = "regio_grid") for col in df.columns])
df.to_sql("regio_grid", con = JPOD_CONN, if_exists="append", index=False)

#### Save and close connection to .db -------------------
JPOD_CONN.commit()
JPOD_CONN.close()
