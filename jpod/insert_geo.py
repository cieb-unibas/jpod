import sys
import navigate as nav
import pandas as pd
import sqlite3 
print(sqlite3.sqlite_version) # has to be >= 3.33.0 in order for the sqlite query to work.

#### establish connection and load data --------------------------------------
DB_DIR = sys.argv[1]
JPOD_CONN = nav.db_connect(db_path = DB_DIR)
df = pd.read_csv(DB_DIR + "augmentation_data/regio_grid.csv", sep = ";")
df[["name_en", "name_de", "name_fr"]] = df[["name_en", "name_de", "name_fr"]].apply(lambda x: x.str.lower()) # lowercase

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

# verify that tables and variables exists:
# insert the regional data to the newly created JPOD table
assert "regio_grid" in nav.get_tables(conn = JPOD_CONN)
assert all([col in nav.get_table_vars(conn = JPOD_CONN, table = "regio_grid") for col in df.columns])
df.to_sql("regio_grid", con = JPOD_CONN, if_exists="append", index=False)

#### match postings to regions --------------------------------------
# create a function to get nuts_codes
def insert_geo_level(insert_table, insert_variable, matching_variable):
    JPOD_QUERY = """
    -- Check for matches with English region expression 
    UPDATE {0}
    SET {1} = rg.{1}
    FROM regio_grid rg
    WHERE {2} = rg.name_en AND {0}.{1} IS NULL;

    -- Check for matches with German region expression 
    UPDATE {0}
    SET {1} = rg.{1}
    FROM regio_grid rg
    WHERE {2} = rg.name_de AND {0}.{1} IS NULL;

    -- Check for matches with French region expression 
    UPDATE {0}
    SET {1} = rg.{1}
    FROM regio_grid rg
    WHERE {2} = rg.name_fr AND {0}.{1} IS NULL;
    """.format(insert_table, insert_variable, matching_variable)

    return JPOD_QUERY


for geo_level in ["nuts_2", "nuts_3"]:
    JPOD_CONN.execute("UPDATE position_characteristics  SET %s = NULL" % (geo_level)) # for updates
    #JPOD_CONN.execute("ALTER TABLE position_characteristics ADD COLUMN %s VARCHAR(5)" % (geo_level))
    for match_var in ["state", "inferred_state"]:
        JPOD_QUERY = insert_geo_level(insert_table="position_characteristics", insert_variable= geo_level, matching_variable=match_var)
        JPOD_CONN.executescript(JPOD_QUERY)
        print("Terminated insertion of geographical level '{0}' based on string-matching in column '{1}'.".format(geo_level, match_var))

# check for "state" and "city" values that could not be matched but are frequent
JPOD_QUERY = """
SELECT state, COUNT(*) AS number_mentions
FROM position_characteristics
GROUP BY state
HAVING nuts_2 IS NULL AND number_mentions > 5000
ORDER BY number_mentions DESC
"""
print("Frequently non-matched 'state' values:")
print(pd.read_sql_query(con=JPOD_CONN, sql=JPOD_QUERY))
JPOD_QUERY = """
SELECT city, state, inferred_state, COUNT(city) AS n_mentions
FROM position_characteristics
GROUP BY city
HAVING nuts_2 IS NULL AND n_mentions > 5000
ORDER BY n_mentions DESC
"""
print("Frequently non-matched 'city' values:")
print(pd.read_sql_query(con=JPOD_CONN, sql=JPOD_QUERY))

# custom matching procedure to grab more rows for the biggest non-matched entities
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

#### Tests
print("Remaining non-matched 'state' values:")
JPOD_QUERY = """
SELECT state, COUNT(*) AS number_mentions
FROM position_characteristics
GROUP BY state
HAVING nuts_2 IS NULL AND number_mentions > 1000
ORDER BY number_mentions DESC
"""
print(pd.read_sql_query(con=JPOD_CONN, sql=JPOD_QUERY))

print("Remaining non-matched 'inferred_state' values:")
JPOD_QUERY = """
SELECT state, COUNT(*) AS number_mentions
FROM position_characteristics
GROUP BY inferred_state
HAVING nuts_2 IS NULL AND number_mentions > 1000
ORDER BY number_mentions DESC
"""
print(pd.read_sql_query(con=JPOD_CONN, sql=JPOD_QUERY))

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

#### Example -------------------
JPOD_QUERY = """
SELECT 
    rg.name_en AS Region, 
    COUNT(*) AS N_Postings,
    COUNT(DISTINCT(company_name)) AS N_Firms
FROM position_characteristics pc
LEFT JOIN 
    (SELECT nuts_2, name_en
    FROM regio_grid
    WHERE regio_abbrev IS NULL AND nuts_3 IS NULL) rg
    ON pc.nuts_2 = rg.nuts_2
GROUP BY pc.nuts_2
HAVING pc.nuts_2 IS NOT NULL
ORDER BY n_postings DESC
"""
print("Total number of postings and distinct firms across regions:")
print(pd.read_sql_query(con=JPOD_CONN, sql=JPOD_QUERY))

#### Save and close connection to .db --------------------------------------
JPOD_CONN.commit()
JPOD_CONN.close()
