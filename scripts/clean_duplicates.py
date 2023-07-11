import os
import sys
import sqlite3
import time

print("Current directory is: " + os.getcwd())
sys.path.append(os.getcwd())

from jpod.datagen import DuplicateCleaner
import jpod.config as config
import jpod.navigate as nav

if __name__ == "__main__":

    # parameters for connecting to JPOD
    JPOD_VERSION = "jpod.db"
    DATA_BATCH = "jobspickr_2023_01"
    RESTRICT_TO_COUNTRIES = ["united kingdom"]
    EXCLUDE_COUNTRIES = None
    DB_DIR = os.path.join(nav.get_path(config.DB_DIRS), JPOD_VERSION)
    JPOD_CONN = sqlite3.connect(database = DB_DIR)
    start = time.perf_counter()
    
    # make sure columns exist in the database
    existing_vars = nav.get_table_vars(conn=JPOD_CONN, table="job_postings")
    for c in ["unique_posting_text", "unique_posting_textlocation"]:
        if c not in existing_vars:
            JPOD_CONN.execute("ALTER TABLE job_postings ADD COLUMN %s VARCHAR(3) DEFAULT 'yes'" % c)
            print("Created new columns %s in table job postings. Ready to perform duplicate cleaning" % c)
        else:
            print("Columns %s exists in table job postings. Ready to perform duplicate cleaning" % c)

    # Load the cleaner and identify duplicates per country-batch:
    print("---------------Starting to identify duplicated postings---------------")
    cleaner = DuplicateCleaner(con = JPOD_CONN, data_batch = 'jobspickr_2023_01', restrict_to_countries = RESTRICT_TO_COUNTRIES)
    JPOD_QUERY = cleaner.duplicate_query(assign_to = "unique_posting_text", levels=["job_description", "inferred_country"])
    cleaner.find_duplicates(query = JPOD_QUERY, commit = True)
    JPOD_QUERY = cleaner.duplicate_query(assign_to = "unique_posting_textlocation", levels=["job_description", "city"])
    cleaner.find_duplicates(query = JPOD_QUERY, commit = True)

    # Save and close connection to JPOD, log the performance of the script
    JPOD_CONN.commit()
    JPOD_CONN.close()
    end = time.perf_counter()
    print("Execution took %f minutes." % round((end - start) / 60, ndigits=3))