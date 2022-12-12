import sys
import time
import sqlite3
try:
    from jpod import navigate as nav
    from jpod import datagen as dg
except:
    import navigate as nav
    import datagen as dg
import pandas as pd

#### connect to JPOD
DB_DIR = sys.argv[1]
DB_VERSION = sys.argv[2]
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)
# JPOD_CONN = sqlite3.connect("C:/Users/nigmat01/Desktop/jpod_test.db")
# JPOD_CONN = sqlite3.connect("C:/Users/matth/Desktop/jpod_test.db")

#### identify exact duplicates:
# test how time-consuming it is to just check all postings for duplicates using the sql DISTINCT
for n in [100000, 500000, 1000000]:
    JPOD_QUERY = """
    SELECT COUNT(DISTINCT(job_description)) AS n_unique_postings
    FROM (
        SELECT job_description
        FROM job_postings
        LIMIT %d
        )
    """ % n
    start_time = time.time()
    n_unique = JPOD_CONN.execute(JPOD_QUERY).fetchone()[0]
    time.sleep(0.2)
    end_time = time.time()
    time_diff = end_time - start_time
    print("Found {} of {} in {} seconds".format(n_unique, n, round(time_diff, 2)))
exit()
# => should actually be directly doable for the overall sample...

# SKETCH:
# 1) create a new column 'duplicated': "ALTER TABLE job_postings ADD column duplicated VARCHAR DEFAULT 'yes'"
# 2) find all unique postings "SELECT unqi_id FROM (SELECT DISTINCT(job_description) FROM job postings)"
# 3) update their `duplicated` value in the table: 

    # UPDATE TABLE job_postings 
    # SET duplicated = 'no' 
    # WHERE uniq_id IN (
    #   SELECT unqi_id 
    #   FROM (
    #       SELECT DISTINCT(job_description) 
    #       FROM job postings
    #       )
    #   ) 