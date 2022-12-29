import sys
import sqlite3
try:
    from jpod.datagen import DuplicateCleaner
except:
    from datagen import DuplicateCleaner

#### connect to JPOD: ------------------------------
DB_DIR = sys.argv[1] # path to the .db
DB_VERSION = sys.argv[2] # .db version
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)
# JPOD_CONN = sqlite3.connect("C:/Users/nigmat01/Desktop/jpod_test.db")
# JPOD_CONN = sqlite3.connect("C:/Users/matth/Desktop/jpod_test.db")

#### prepare the .db: ------------------------------
new_cols = [
    "unique_posting_text", # exactly identical text
    "unique_posting_textlocation", # identical text AND identical city
    ]

existing_cols = [x[1] for x in JPOD_CONN.execute("PRAGMA table_info(job_postings)").fetchall()]

for c in new_cols:
    if c in existing_cols:
        JPOD_CONN.execute("ALTER TABLE job_postings DROP COLUMN %s" % c)
        print("Dropped column %s" % c)
        JPOD_CONN.commit()
    else:
        JPOD_CONN.execute("ALTER TABLE job_postings ADD COLUMN %s VARCHAR(3) DEFAULT 'yes'" % c)
        print("Added column %s" % c)
        JPOD_CONN.commit()

#### Identify duplicates: ------------------------------
cleaner = DuplicateCleaner(con = JPOD_CONN, data_batch = 'jobspickr_2022_01')

# exactly identical text
JPOD_QUERY = cleaner.duplicate_query(assign_to = "unique_posting_text", levels=["job_description"])
cleaner.find_duplicates(query=JPOD_QUERY)

# exactly identical text and city location
JPOD_QUERY = cleaner.duplicate_query(assign_to = "unique_posting_textlocation", levels=["job_description", "city"])
cleaner.find_duplicates(query=JPOD_QUERY)

#### Test and summarize: -----------------------------
for c in new_cols:
    assert c in [x[1] for x in JPOD_CONN.execute("PRAGMA table_info(job_postings)").fetchall()], "Failed to create column '%s' in Table job_postings" %c
    n_duplicated = JPOD_CONN.execute("SELECT COUNT(*) FROM job_postings WHERE %s == 'yes'" % c).fetchone()[0]
    n_unique = JPOD_CONN.execute("SELECT COUNT(*) FROM job_postings WHERE %s == 'no'" % c).fetchone()[0]
    print("Found %d unique and %d duplicated postings in JPOD for column '%s'" % (n_unique, n_duplicated, c))
