import sys
sys.path.append("./jpod")
import navigate as nav
import pandas as pd

# establish connection and load data
JPOD_CONN = nav.db_connect("C:/Users/matth/Desktop/")
df = pd.read_csv("C:/Users/matth/Desktop/ch_regio_grid.csv", sep = ";")
df = df.apply(lambda x: x.str.lower()) # lowercase

# create table in JPOD:
JPOD_QUERY = """
DROP TABLE IF EXISTS 
regio_grid;
"""
JPOD_CONN.execute(JPOD_QUERY)

JPOD_QUERY = """
CREATE TABLE regio_grid (
name_en TEXT NOT NULL PRIMARY KEY,
name_de TEXT,
name_fr TEXT,
regio_abbrev VARCHAR(5),
nuts_0 VARCHAR(2),
nuts_1 VARCHAR(3),
nuts_2 VARCHAR(4),
nuts_3 VARCHAR(5),
oecd_tl1 VARCHAR(2),
oecd_tl2 VARCHAR(4),
oecd_tl3 VARCHAR(5)
);
"""
JPOD_CONN.execute(JPOD_QUERY)
JPOD_CONN.commit()

# verify that tables and variables exists:
assert "regio_grid" in nav.get_tables(conn = JPOD_CONN)
assert all([col in nav.get_table_vars(conn = JPOD_CONN, table = "regio_grid") for col in df.columns])

# insert the data to JPOD the newly created JPOD table
df.to_sql("regio_grid", con = JPOD_CONN, if_exists="append", index=False)
JPOD_QUERY = """
SELECT *
FROM regio_grid
"""
print("New table 'regio_grid' inserted to JPOD:")
pd.read_sql_query(con=JPOD_CONN, sql=JPOD_QUERY)

#### match postings to regions -------------------

# for development: 
# clone pc to experiment:
JPOD_QUERY = """
DROP TABLE IF EXISTS test_nuts;
CREATE TABLE test_nuts AS
SELECT *
FROM position_characteristics;
"""
JPOD_CONN.executescript(JPOD_QUERY)
nav.get_table_vars(conn=JPOD_CONN, table = "test_nuts")

#### NUTS:
JPOD_QUERY ="""
UPDATE test_nuts
SET nuts_2 = rg.nuts_2, 
    nuts_3 = rg.nuts_3
    FROM ((
        SELECT uniq_id, inferred_state
        FROM position_characteristics
        ) pc
    LEFT JOIN (
        SELECT name_en, nuts_2, nuts_3
        FROM regio_grid
        WHERE nuts_3 IS NOT NULL AND nuts_2 IS NOT NULL
        ) rg on pc.inferred_state = rg.name_en)
WHERE uniq_id = pc.uniq_id
"""
JPOD_CONN.execute(JPOD_QUERY)


JPOD_QUERY="""
SELECT *
FROM (
    SELECT uniq_id, inferred_state
    FROM test_nuts
    ) tn
LEFT JOIN (
    SELECT name_en, nuts_2, nuts_3
        FROM regio_grid
        WHERE nuts_3 IS NOT NULL AND nuts_2 IS NOT NULL
        ) rg on pc.inferred_state = rg.name_en)
WHERE uniq_id = pc.uniq_id
"""


# a) create the new columns:
JPOD_QUERY = """
ALTER TABLE test_nuts
ADD COLUMN nuts_2 VARCHAR(4)
"""
JPOD_CONN.execute(JPOD_QUERY)

JPOD_QUERY = """
ALTER TABLE test_nuts
ADD COLUMN nuts_3 VARCHAR(5)
"""
JPOD_CONN.execute(JPOD_QUERY)

JPOD_QUERY = """
SELECT
FROM test_nuts
"""
pd.read_sql_query(con=JPOD_CONN, sql=JPOD_QUERY)




JPOD_QUERY = """
UPDATE test_nuts
SET nuts_2 = (
    SELECT nuts_2
    FROM (
        SELECT inferred_state
        FROM test_nuts
        ) tn
    LEFT JOIN (
        SELECT name_en, nuts_2
        FROM regio_grid
        WHERE nuts_3 IS NOT NULL AND nuts_2 IS NOT NULL
        ) rg ON tn.inferred_state = rg.name_en
    )
"""
pd.read_sql_query(con=JPOD_CONN, sql=JPOD_QUERY)
JPOD_CONN.execute(JPOD_QUERY)



