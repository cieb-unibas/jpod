import sys
import navigate as nav
import datagen as dg
import pandas as pd
import sqlite3 
assert float(sqlite3.sqlite_version[:4]) >= 3.33, "SQLITE version must be 3.33.0 or higher. Please upgrade."

#### establish connection and load data --------------------------------------
DB_DIR = sys.argv[1]
DB_VERSION = sys.argv[2]
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)
df = pd.read_csv(DB_DIR + "augmentation_data/regio_grid.csv", sep = ";")
df[["name_en", "name_de", "name_fr"]] = df[["name_en", "name_de", "name_fr"]].apply(lambda x: x.str.lower())

#### create regio table in JPOD --------------------------------------
JPOD_QUERY = """
DROP TABLE IF EXISTS regio_grid;
CREATE TABLE regio_grid (
name_en TEXT NOT NULL,
name_de TEXT,
name_fr TEXT,
regio_abbrev VARCHAR(5),
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

#### match postings to regions --------------------------------------
for geo_level in ["nuts_2", "nuts_3"]:
    if geo_level in get_table_vars(conn = JPOD_CON, table = "position_characteristics"):
        JPOD_CONN.execute("UPDATE position_characteristics  SET %s = NULL" % (geo_level))
    else:
        JPOD_CONN.execute("ALTER TABLE position_characteristics ADD COLUMN %s VARCHAR(5)" % (geo_level))
    for match_var in ["state", "inferred_state"]:
        JPOD_QUERY = dg.geo_query(insert_table="position_characteristics", insert_variable= geo_level, matching_variable=match_var)
        JPOD_CONN.executescript(JPOD_QUERY)
        print("Terminated insertion of geographical level '{0}' based on string-matching in column '{1}'.".format(geo_level, match_var))


#### custom matching procedure to grab more rows for the biggest non-matched entities --------------------------------------
JPOD_QUERY = """
-- St. Gallen:
UPDATE position_characteristics
SET nuts_2 = "CH05", nuts_3 = "CH055"
WHERE 
    (state LIKE "%gallen%" OR inferred_state = "%gallen%" 
    OR state LIKE "% gall%" OR state = "sg" OR city = "buchs"
    OR city = "wil")
    AND nuts_2 IS NULL;

-- Geneva:
UPDATE position_characteristics
SET nuts_2 = "CH01", nuts_3 = "CH013"
WHERE 
    (state LIKE "%geneve%" OR inferred_state = "geneve" 
    OR state LIKE "% genf%" OR state = "ge")
    AND nuts_2 IS NULL;

-- Graubünden:
UPDATE position_characteristics
SET nuts_2 = "CH05", nuts_3 = "CH056"
WHERE 
    (state = "gr" OR city = "chur")
    AND nuts_2 IS NULL;

-- Bern:
UPDATE position_characteristics
SET nuts_2 = "CH02", nuts_3 = "CH021"
WHERE 
    (city = "biel" OR city = "bienne" OR city = "ittigen")
    AND nuts_2 IS NULL;

-- Basel-Stadt:
UPDATE position_characteristics
SET nuts_2 = "CH03", nuts_3 = "CH031"
WHERE 
    (state LIKE "%baselstadt%" OR inferred_state = "bs")
    AND nuts_2 IS NULL;

-- Basel-Landschaft:
UPDATE position_characteristics
SET nuts_2 = "CH03", nuts_3 = "CH032"
WHERE 
    (state LIKE "%basellandschaft%" OR inferred_state = "bl")
    AND nuts_2 IS NULL;

-- Aargau
UPDATE position_characteristics
SET nuts_2 = "CH03", nuts_3 = "CH033"
WHERE 
    (state = "argovia" OR inferred_state = "ag")
    AND nuts_2 IS NULL;

UPDATE position_characteristics
SET nuts_2 = "CH03"
WHERE 
    (state LIKE "%basel%" OR inferred_state LIKE "%basel%")
    AND nuts_2 IS NULL;

-- Freiburg:
UPDATE position_characteristics
SET nuts_2 = "CH02", nuts_3 = "CH022"
WHERE 
    (state LIKE "%freiburg")
    AND nuts_2 IS NULL;
"""
JPOD_CONN.executescript(JPOD_QUERY)
print("Refined matching procedure for frequent values successful.")

#### summarize -------------------
JPOD_QUERY = """
SELECT 
    COUNT(*) AS n_matched,
    (SELECT COUNT(*) FROM position_characteristics) AS n_total
FROM position_characteristics
WHERE nuts_2 IS NOT NULL AND nuts_3 IS NOT NULL
"""
tmp = pd.read_sql_query(con=JPOD_CONN, sql=JPOD_QUERY)
tmp["share"] = tmp["n_matched"] / tmp["n_total"]
print("{0} of {1} job postings assigned to geographical levels (share of {2}).".format(*(tmp.iloc[0,:])))

#### Save and close connection to .db --------------------------------------
JPOD_CONN.commit()
JPOD_CONN.close()