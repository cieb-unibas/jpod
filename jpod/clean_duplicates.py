import sys
import time
import sqlite3

#### connect to JPOD
DB_DIR = sys.argv[1]
DB_VERSION = sys.argv[2]
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)
# JPOD_CONN = sqlite3.connect("C:/Users/nigmat01/Desktop/jpod_test.db")
# JPOD_CONN = sqlite3.connect("C:/Users/matth/Desktop/jpod_test.db")

#### Identify exact duplicates: ------------------------------

# test how time-consuming it is to just check all postings for duplicates using the sql DISTINCT
for n in [100000, 500000, 1000000]:
    JPOD_QUERY = """
    SELECT COUNT(*)
    FROM(
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY job_description ORDER BY uniq_id) as rnr
        FROM job_postings
        LIMIT %d
        )
    WHERE rnr = 1
    """ % n
    start_time = time.time()
    n_unique = JPOD_CONN.execute(JPOD_QUERY).fetchone()[0]
    time.sleep(0.2)
    end_time = time.time()
    time_diff = end_time - start_time
    print("Found {} of {} in {} seconds".format(n_unique, n, round(time_diff, 2)))
exit()

# test if exists
if "duplicated" in [x[1] for x in JPOD_CONN.execute("PRAGMA table_info(job_postings)").fetchall()]:
    JPOD_CONN.execute("ALTER TABLE job_postings DROP COLUMN duplicated")

# define query to update the table and identify uniq_postings:
JPOD_QUERY="""
ALTER TABLE job_postings ADD COLUMN duplicated VARCHAR(3) DEFAULT 'yes';
UPDATE job_postings 
SET duplicated = 'no' 
WHERE uniq_id IN (
    SELECT uniq_id
    FROM(
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY job_description ORDER BY uniq_id) as rnr
        FROM job_postings
        )
    WHERE rnr = 1
    );
"""
JPOD_CONN.executescript(JPOD_QUERY)

# test:
assert "duplicated" in [x[1] for x in JPOD_CONN.execute("PRAGMA table_info(job_postings)").fetchall()], "Fauled to create column 'duplicated' in Table job_postings"
n_duplicated = JPOD_CONN.execute("SELECT COUNT(*) FROM job_postings WHERE duplicated == 'yes'").fetchone()[0]
n_unique = JPOD_CONN.execute("SELECT COUNT(*) FROM job_postings WHERE duplicated == 'no'").fetchone()[0]
print("Found %d unique and %d duplicated postings in JPOD." % (n_unique, n_duplicated))



#### Identify exact duplicates: ------------------------------
