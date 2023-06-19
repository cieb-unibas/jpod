import sqlite3
import os
import sys
import time

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

    print("---------------Updating Bloom technologies---------------")
    start = time.perf_counter()

    # load keywords
    df = pd.read_csv("data/bloom_tech.csv")
    df["keyword_en"] = [w.replace(r"%", r"@%") for w in df["keyword_en"]]
    # for v in ["en", "de", "fr", "it"]:
    #     df["keyword_" + v] = [w.replace(r"%", r"@%") for w in df["keyword_" + v]]
    #     df["keyword_" + v] = [w.replace(r"_", r"@_") for w in df["keyword_" + v]]
    #     df["keyword_" + v] = [w.replace(r"'", r"''") for w in df["keyword_" + v]]

    res = pd.DataFrame()
    n_searchable = JPOD_CONN.execute("SELECT COUNT(uniq_id) FROM job_postings WHERE data_batch = '%s'" % DATA_BATCH).fetchone()[0]
    print("Searching for keywords in job descriptions of data batch '%s' consisting of %d postings" % (DATA_BATCH, n_searchable))

    # loop over techfields and extract postings that contain their keywords
    for field in list(set(df["bloom_field"])):
        print("Searching job postings in the field of: %s" % field)
        # define keywords and SQL query for the techfield:
        
        # keywords = []
        # for v in ["en", "de", "fr", "it"]:
        #     keywords += list(df[(df.bloom_field) == field]["keyword_" + v])
        #     keywords = list(set(keywords))
        keywords = list(df[(df.bloom_field) == field]["keyword_en"])

        JPOD_QUERY = dg.keyword_query(
            keywords = keywords,
            matching_column = "job_description",
            data_batch = DATA_BATCH
            )

        # retrieve and annotate postings with a connection to the techfield:
        tmp = pd.read_sql(con = JPOD_CONN, sql=JPOD_QUERY)
        if len(tmp) > 0:
            tmp["bloom_field"] = field
            print("Searching for job postings in the field {0} completed. Number of postings retrieved: {1}".format(field, len(tmp)))
        else:
            print("Searching for job postings in the field %s completed. Number of postings retrieved: 0" %field)
        
        # add to the final dataset
        res = pd.concat([res, tmp], axis=0)
    
    assert len(res.columns) == 2, "Resulting dataset has %d columns - %d are expected." % (len(res.columns), 2)
    print("Keyword search completet for all disruptive technology fields by Bloom et al. (2020).")
    
    # prepare the final data and if it does not exist, create new JPOD table "bloom_tech"
    res = pd.merge(res, pd.read_csv("data/raw_data/bloom_fields.csv"), on = "bloom_field")
    if "bloom_tech" not in nav.get_tables(conn = JPOD_CONN):
        JPOD_QUERY = """
        CREATE TABLE bloom_tech(
        uniq_id VARCHAR(32) NOT NULL,
        bloom_field VARCHAR(30),
        bloom_code INT,
        PRIMARY KEY (uniq_id, bloom_code)
        );
        """
        JPOD_CONN.executescript(JPOD_QUERY)

    # insert data into the table, commit and close connection to JPOD
    try:
        res.to_sql(name = "bloom_tech", con = JPOD_CONN, if_exists="append", index=False)
    except:
        exiting_pkeys = dg.retrieve_pkeys(table = "bloom_tech", p_key = ["uniq_id", "bloom_code"], conn = JPOD_CONN)
        res = pd.concat([exiting_pkeys, res], axis = 0).\
            drop_duplicates(subset = ["uniq_id", "bloom_code"], keep = "last").\
                dropna(subset = ["bloom_field"])
        res.to_sql(name = "bloom_tech", con = JPOD_CONN, if_exists = "append", index = False)
    JPOD_CONN.commit()
    JPOD_CONN.close()
    print("Successfully inserted information regarding disruptive technology fields by Bloom et al. (2020).")
    end = time.perf_counter()
    print("Execution took %f minutes." % round((end - start) / 60, ndigits=3))
