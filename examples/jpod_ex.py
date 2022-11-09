import pandas as pd
import sqlite3

DB_DIR = "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"

def get_tables(conn):
    res = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    res = [col[0] for col in res.fetchall()]
    return res
    
def get_table_vars(conn, table):
    res = conn.execute("PRAGMA table_info({});".format(table))
    res = [col[1] for col in res.fetchall()]
    return res

#### connect to the database and get its current structure ---------------------
JPOD_CONN = sqlite3.connect(DB_DIR + "jpod.db") # complete database
# JPOD_CONN = sqlite3.connect(DB_DIR + "jpod_test.db") # random subsample for 0.5% of all postings
TABLES_VARS = {}
for table in get_tables(conn=JPOD_CONN):
    TABLES_VARS[table] = get_table_vars(table = table, conn = JPOD_CONN)

# EXAMPLE 1: number of job adds by job board
jpod_query = """
SELECT job_board as plattform, COUNT(*) as n_postings 
FROM job_postings 
GROUP BY job_board
ORDER BY n_postings DESC;
"""
pd.read_sql_query(jpod_query, con=JPOD_CONN) # Opion 1: read via pandas read_sql_query() method
# pd.DataFrame(JPOD_CONN.execute(jpod_query).fetchall(), columns=["plattform", "n_postings"]) # Opion 2: read via sqlite execute() method

# EXAMPLE 2: Biggest 10 cities with their total number of postings
jpod_query = """
SELECT city, COUNT(city) n_postings
FROM position_characteristics 
GROUP BY city
HAVING city IS NOT "nan"
ORDER BY n_postings DESC
LIMIT 10;
"""
pd.read_sql_query(jpod_query, con=JPOD_CONN)

# EXAMPLE 3: Job positions from a specific company (Migros) in a given city (Bern)
jpod_query = """
SELECT company_name, city, inferred_job_title AS job_title
FROM position_characteristics
WHERE city LIKE "bern" AND company_name LIKE "%migros%"
"""
pd.read_sql_query(jpod_query, con=JPOD_CONN)

# EXAMPLE 4: Job postings and company information using the term "machine learning"
KEYWORD = "machine learning"
jpod_query = """
SELECT pc.company_name, pc.city, jp.job_title, jp.job_description
FROM (
    SELECT uniq_id, job_title, job_description
    FROM job_postings
    WHERE job_description LIKE "%{}%"
    ) jp
LEFT JOIN position_characteristics pc ON pc.uniq_id = jp.uniq_id;
""".format(KEYWORD)
print("Employers asking about '{}': ".format(KEYWORD))
pd.read_sql_query(jpod_query, con=JPOD_CONN)

# EXAMPLE 5: Top 20 companies in terms of the number of postings using the term "machine learning"
KEYWORD = "machine learning"
jpod_query = """
SELECT pc.company_name, COUNT(*) as n_postings
FROM (
    SELECT uniq_id, job_title, job_description
    FROM job_postings
    WHERE job_description LIKE "%{}%"
    ) jp
LEFT JOIN position_characteristics pc ON pc.uniq_id = jp.uniq_id
GROUP BY pc.company_name
ORDER BY n_postings DESC
LIMIT 20;
""".format(KEYWORD)
print("Employers asking about '{}': ".format(KEYWORD))
pd.read_sql_query(jpod_query, con=JPOD_CONN)


#### close connection to .db -------------------
JPOD_CONN.close()