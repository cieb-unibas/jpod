import os
import sys
import sqlite3

print("Current directory is: " + os.getcwd())
sys.path.append(os.getcwd())
from jpod.datagen import DuplicateCleaner
import jpod.config as config
import jpod.navigate as nav

if __name__ == "__main__":

    # parameters for connecting to JPOD
    JPOD_VERSION = "jpod_test.db"
    DATA_BATCH = "jobspickr_2023_01"
    DB_DIR = os.path.join(nav.get_path(config.DB_DIRS), JPOD_VERSION)
    JPOD_CONN = sqlite3.connect(database = DB_DIR)
    print("---------------Starting to identify duplicated postings---------------")
    
    # check if the columns already exist and set them to their default values:
    new_cols = [
        "unique_posting_text", # no exactly identical text
        "unique_posting_textlocation", # no identical text within city
        ]
    existing_cols = nav.get_table_vars(conn = JPOD_CONN, table = "job_postings")
    for c in new_cols:
        if c in existing_cols:
            JPOD_CONN.execute("UPDATE job_postings SET %s = 'yes' WHERE data_batch == '%s'" % (c, DATA_BATCH))
            print("Set values in column '%s' for data batch '%s' to default value 'yes'." % (c, DATA_BATCH))
            JPOD_CONN.commit()
        else:
            JPOD_CONN.execute("ALTER TABLE job_postings ADD COLUMN %s VARCHAR(3) DEFAULT 'yes'" % c)
            print("Initialized column %s" % c)
            JPOD_CONN.commit()

    # Identify duplicates:
    cleaner = DuplicateCleaner(con = JPOD_CONN, data_batch = 'jobspickr_2023_01')
    JPOD_QUERY = cleaner.duplicate_query(assign_to = "unique_posting_text", levels=["job_description", "inferred_country"])
    cleaner.find_duplicates(query = JPOD_QUERY, commit = True)
    JPOD_QUERY = cleaner.duplicate_query(assign_to = "unique_posting_textlocation", levels=["job_description", "city"])
    cleaner.find_duplicates(query = JPOD_QUERY, commit = True)

    # test and summarize:
    for c in new_cols:
        assert c in nav.get_table_vars(conn=JPOD_CONN, table="job_postings"), "Failed to create column '%s' in Table job_postings" %c
        n_duplicated = JPOD_CONN.execute("SELECT COUNT(*) FROM job_postings WHERE data_batch == '%s' AND %s == 'no'" % (DATA_BATCH, c)).fetchone()[0]
        n_unique = JPOD_CONN.execute("SELECT COUNT(*) FROM job_postings WHERE data_batch == '%s' AND %s == 'yes'" % (DATA_BATCH,c)).fetchone()[0]
        print("Found %d unique and %d duplicated postings in JPOD for column '%s' and data batch '%s'" % (n_unique, n_duplicated, c, DATA_BATCH))

    #### Save and close connection to .db
    JPOD_CONN.commit()
    JPOD_CONN.close()