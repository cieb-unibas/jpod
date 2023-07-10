import os
import sys
import sqlite3
import time

print("Current directory is: " + os.getcwd())
sys.path.append(os.getcwd())
from jpod.datagen import DuplicateCleaner, _set_country_batch_to_default
import jpod.config as config
import jpod.navigate as nav

if __name__ == "__main__":

    # parameters for connecting to JPOD
    JPOD_VERSION = "jpod.db"
    DATA_BATCH = "jobspickr_2023_01"
    RESTRICT_TO_COUNTRIES = ["switzerland"]
    EXCLUDE_COUNTRIES = None
    DB_DIR = os.path.join(nav.get_path(config.DB_DIRS), JPOD_VERSION)
    JPOD_CONN = sqlite3.connect(database = DB_DIR)
    
    print("---------------Starting to identify duplicated postings---------------")
    start = time.perf_counter()
    # check if the columns already exist and set them to their default values:
    new_cols = [
        "unique_posting_text", # no exactly identical text
        "unique_posting_textlocation", # no identical text within city
        ]
    existing_cols = nav.get_table_vars(conn = JPOD_CONN, table = "job_postings")
    for c in new_cols:
        query = _set_country_batch_to_default(
            col = c, defaul_value='yes', data_batch=DATA_BATCH, 
            restrict_to_countries= RESTRICT_TO_COUNTRIES,
            exclude_countries=EXCLUDE_COUNTRIES)
        JPOD_CONN.execute(query)
        # log:
        if RESTRICT_TO_COUNTRIES:
            print("Set values in column '%s' for countries %s from data batch '%s' to default value 'yes'." % (c, ", ".join(RESTRICT_TO_COUNTRIES), DATA_BATCH))
        if EXCLUDE_COUNTRIES:
            print("Set values in column '%s' from data batch '%s' to default value 'yes' except for countries %s." % (c, DATA_BATCH,", ".join(EXCLUDE_COUNTRIES)))
        if not RESTRICT_TO_COUNTRIES or EXCLUDE_COUNTRIES:
            print("Set values in column '%s' for data batch '%s' to default value 'yes'." % (c, DATA_BATCH))

    # Identify duplicates:
    cleaner = DuplicateCleaner(con = JPOD_CONN, data_batch = 'jobspickr_2023_01', restrict_to_countries = RESTRICT_TO_COUNTRIES)
    JPOD_QUERY = cleaner.duplicate_query(assign_to = "unique_posting_text", levels=["job_description", "inferred_country"])
    cleaner.find_duplicates(query = JPOD_QUERY, commit = True)
    JPOD_QUERY = cleaner.duplicate_query(assign_to = "unique_posting_textlocation", levels=["job_description", "city"])
    cleaner.find_duplicates(query = JPOD_QUERY, commit = True)

    # test and summarize:
    for c in new_cols:
        assert c in nav.get_table_vars(conn=JPOD_CONN, table="job_postings"), "Failed to create column '%s' in Table job_postings" %c
        n_duplicated = JPOD_CONN.execute("SELECT COUNT(uniq_id) FROM job_postings WHERE data_batch == '%s' AND %s == 'no'" % (DATA_BATCH, c)).fetchone()[0]
        n_unique = JPOD_CONN.execute("SELECT COUNT(uniq_id) FROM job_postings WHERE data_batch == '%s' AND %s == 'yes'" % (DATA_BATCH, c)).fetchone()[0]
        print("Found %d unique and %d duplicated postings in JPOD for column '%s' and data batch '%s'" % (n_unique, n_duplicated, c, DATA_BATCH))

    #### Save and close connection to .db
    JPOD_CONN.commit()
    JPOD_CONN.close()
    end = time.perf_counter()
    print("Execution took %f minutes." % round((end - start) / 60, ndigits=3))

# no restrictions
# Found 81301 unique and 18699 duplicated postings in JPOD for column 'unique_posting_text' and data batch 'jobspickr_2023_01'
# Found 84885 unique and 15115 duplicated postings in JPOD for column 'unique_posting_textlocation' and data batch 'jobspickr_2023_01'

# after setting CH/GER to default
# Found 95619 unique and 4381 duplicated postings in JPOD for column 'unique_posting_text' and data batch 'jobspickr_2023_01'
# Found 98187 unique and 1813 duplicated postings in JPOD for column 'unique_posting_textlocation' and data batch 'jobspickr_2023_01'

# after cleaning CH/GER
# Found 81301 unique and 18699 duplicated postings in JPOD for column 'unique_posting_text' and data batch 'jobspickr_2023_01'
# Found 84887 unique and 15113 duplicated postings in JPOD for column 'unique_posting_textlocation' and data batch 'jobspickr_2023_01'

# after setting everything to default except CH/GER
# Found 85682 unique and 14318 duplicated postings in JPOD for column 'unique_posting_text' and data batch 'jobspickr_2023_01'
# Found 86700 unique and 13300 duplicated postings in JPOD for column 'unique_posting_textlocation' and data batch 'jobspickr_2023_01'

# after cleaning everything except CH/GER
# Found 81301 unique and 18699 duplicated postings in JPOD for column 'unique_posting_text' and data batch 'jobspickr_2023_01'
# Found 84888 unique and 15112 duplicated postings in JPOD for column 'unique_posting_textlocation' and data batch 'jobspickr_2023_01'
