import os
import sqlite3
import sys

import pandas as pd

print("Current directory is: " + os.getcwd())
sys.path.append(os.getcwd())
import jpod.datagen as dg
import jpod.config as config
import jpod.navigate as nav


if __name__ == "__main__":

    # parameters for connecting to JPOD
    JPOD_VERSION = "jpod_test.db"
    DATA_BATCH = "jobspickr_2023_01"

    DB_DIR = os.path.join(nav.get_path(config.DB_DIRS), JPOD_VERSION)
    JPOD_CONN = sqlite3.connect(database = DB_DIR)
    HOME = os.path.abspath(os.path.join(__file__, os.pardir, os.pardir))
    os.chdir(HOME)

    print("---------------Updating AI related postings---------------")

    # define keywords
    keywords = dg.load_and_clean_keywords(keyword_file = "data/acemoglu_ai_keywords.csv", multilingual=False)

    # define sql query
    JPOD_QUERY = dg.keyword_query(
        keywords = keywords, 
        matching_column = "job_description",
        data_batch = DATA_BATCH
        )
    
    # retrieve postings with a connection to AI:
    n_searchable = JPOD_CONN.execute("SELECT COUNT(uniq_id) FROM job_postings WHERE data_batch = '%s'" % DATA_BATCH).fetchone()[0]
    print("Searching for keywords in job descriptions of data batch '%s' consisting of %d postings" % (DATA_BATCH, n_searchable))
    res = pd.read_sql(con = JPOD_CONN, sql=JPOD_QUERY)
    if len(res) > 0:
        print("Searching for job postings with connection to AI completed. Number of postings retrieved: {}".format(len(res)))
    else:
        print("Searching for job postings with connection to AI completed. Number of postings retrieved: 0")

    # if necessary, create new table "acemoglu_ai" and insert data into the table
    if "acemoglu_ai" not in nav.get_tables(conn = JPOD_CONN):
        JPOD_QUERY = """
        CREATE TABLE acemoglu_ai(
            uniq_id VARCHAR(32) NOT NULL,
            PRIMARY KEY (uniq_id)
            );
        """
        JPOD_CONN.executescript(JPOD_QUERY)
    
    # insert data to JPOD
    try:
        res.to_sql(name = "acemoglu_ai", con = JPOD_CONN, if_exists="append", index=False)
    except:
        exiting_pkeys = dg.retrieve_pkeys(table = "acemoglu_ai", p_key = "uniq_id", conn = JPOD_CONN)
        res = dg.unique_records(df = res, df_identifier="uniq_id", existing_pkeys=exiting_pkeys)
        res.to_sql(name = "acemoglu_ai", con = JPOD_CONN, if_exists = "append", index = False)

    # Save and close connection to .db 
    JPOD_CONN.commit()
    JPOD_CONN.close()