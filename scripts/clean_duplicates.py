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
    DATA_BATCH = config.BATCH_VERSION
    DB_DIR = os.path.join(nav.get_path(config.DB_DIRS), JPOD_VERSION)
    JPOD_CONN = sqlite3.connect(database = DB_DIR)

    RESTRICT_TO_GEO_UNITS = False
    EXCLUDE_GEO_UNITS = False
    
    if RESTRICT_TO_GEO_UNITS:
        print("Cleaning for the following geographic entities: ", ", ".join(RESTRICT_TO_GEO_UNITS))
    if EXCLUDE_GEO_UNITS:
        print("Cleaning for all geographic entities except the following: ", ", ".join(RESTRICT_TO_GEO_UNITS))
       
    # Load the cleaner:
    start = time.perf_counter()
    print("---------------Starting to identify duplicated postings---------------")
    cleaner = DuplicateCleaner(con = JPOD_CONN, data_batch = 'jobspickr_2023_01', restrict_scope_by = "inferred_country", restrict_to_geo_units = RESTRICT_TO_GEO_UNITS)
    # identify duplicates at the country-level
    JPOD_QUERY = cleaner.duplicate_query(assign_to = "unique_posting_text", levels=["job_description"])
    cleaner.find_duplicates(query = JPOD_QUERY, commit = True)
    # identify duplicates at the city-level
    JPOD_QUERY = cleaner.duplicate_query(assign_to = "unique_posting_textlocation", levels=["city", "job_description"])
    cleaner.find_duplicates(query = JPOD_QUERY, commit = True)

    # Save and close connection to JPOD, log the performance of the script
    JPOD_CONN.commit()
    JPOD_CONN.close()
    end = time.perf_counter()
    print("Execution took %f minutes." % round((end - start) / 60, ndigits=3))