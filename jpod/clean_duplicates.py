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

# test how time-consuming it is to just check all postings for duplicates
for n in [100000, 200000, 400000]:
    JPOD_QUERY = """
    SELECT COUNT(*)
    FROM(
        SELECT uniq_id, city, job_description,
        ROW_NUMBER() OVER (PARTITION BY job_description, city ORDER BY uniq_id) as rnr
        FROM(
            SELECT jp.uniq_id, jp.job_description, pc.city
            FROM job_postings jp
            LEFT JOIN position_characteristics pc on jp.uniq_id = pc.uniq_id
            WHERE jp.data_batch = 'jobspickr_2022_01'
            LIMIT %d
            )
        )
    WHERE rnr > 1
    """ % n
    start_time = time.time()
    n_unique = JPOD_CONN.execute(JPOD_QUERY).fetchone()[0]
    end_time = time.time()
    time_diff = end_time - start_time
    print("Found {} duplicated postings from a total set of {} in {} seconds".format(n_unique, n, round(time_diff, 2)))
exit()

# test if new column already exists -----------------------------
existing_cols = [x[1] for x in JPOD_CONN.execute("PRAGMA table_info(job_postings)").fetchall()]
# if "duplicated" in existing_cols:
#     JPOD_CONN.execute("ALTER TABLE job_postings DROP COLUMN duplicated")
if "duplicated" not in existing_cols:
    JPOD_CONN.execute("ALTER TABLE job_postings ADD COLUMN duplicated VARCHAR(3) DEFAULT 'no'")


# define query to update the table and identify duplicated postings: -----------------------------

class duplicate_cleaner():
    """
    Clean and identify duplicated job postings.
    """
    def __init__(self, con, data_batch):
        self.batch = data_batch
        self.con = con

    def duplicate_query(self):
        """
        SQL-Query to identify duplicated job postings
        """
        self.query = """
        UPDATE job_postings 
        SET duplicated = 'yes' 
        WHERE uniq_id IN (
            SELECT uniq_id
            FROM(
                SELECT uniq_id,
                ROW_NUMBER() OVER (PARTITION BY job_description, city ORDER BY uniq_id) as rnr
                FROM(
                    SELECT jp.uniq_id, jp.job_description, pc.city
                    FROM job_postings jp
                    LEFT JOIN position_characteristics pc on jp.uniq_id = pc.uniq_id
                    WHERE jp.data_batch = '%s' 
                    )
                )
            WHERE rnr > 1
        );
        """ % self.batch

    def find_duplicates(self):
        """
        Run SQL-Query to identify and mark duplicated job postings in JPOD
        """
        self.duplicate_query()
        self.con.execute(self.query)
        print("Duplicate cleaning successful")

cleaner = duplicate_cleaner(con = JPOD_CONN, data_batch = 'jobspickr_2022_01')
cleaner.find_duplicates()

# test and summarize: -----------------------------
assert "duplicated" in [x[1] for x in JPOD_CONN.execute("PRAGMA table_info(job_postings)").fetchall()], "Failed to create column 'duplicated' in Table job_postings"
n_duplicated = JPOD_CONN.execute("SELECT COUNT(*) FROM job_postings WHERE duplicated == 'yes'").fetchone()[0]
n_unique = JPOD_CONN.execute("SELECT COUNT(*) FROM job_postings WHERE duplicated == 'no'").fetchone()[0]
print("Found %d unique and %d duplicated postings in JPOD." % (n_unique, n_duplicated))