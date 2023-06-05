import sqlite3
import os

import pandas as pd

import datagen as dg
import navigate as nav
import config
# import jpod.datagen as dg
# import jpod.navigate as nav
# import jpod.config as config


if __name__ == "__main__":

    # parameters for connecting to JPOD
    JPOD_VERSION = "jpod_test.db"
    DATA_BATCH = "jobspickr_2023_01"

    DB_DIR = os.path.join(nav.get_path(config.DB_DIRS), JPOD_VERSION)
    JPOD_CONN = sqlite3.connect(database = DB_DIR)
    HOME = os.path.abspath(os.path.join(__file__, os.pardir, os.pardir))
    os.chdir(HOME)

    print("---------------Updating Bloom technologies---------------")
    
    # load keywords
    df = pd.read_csv("data/bloom_tech.csv")
    for v in ["en", "de", "fr", "it"]:
        df["keyword_" + v] = [w.replace(r"%", r"@%") for w in df["keyword_" + v]]
        df["keyword_" + v] = [w.replace(r"_", r"@_") for w in df["keyword_" + v]]
        df["keyword_" + v] = [w.replace(r"'", r"''") for w in df["keyword_" + v]]

    # loop over techfields and extract postings that contain their keywords
    res = pd.DataFrame()

    for field in set(df["bloom_field"]):
        print("Searching job postings in the field of: %s" %field)
        # define keywords and SQL query for the techfield:
        keywords = []
        for v in ["en", "de", "fr", "it"]:
            keywords += list(df[(df.bloom_field) == field]["keyword_" + v])
            keywords = list(set(keywords))

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
    print("Keyword search completet for all disruptive technology fields by Bloom et al. (2020).")
    
    # prepare the final data and if it does not exist, create new JPOD table "bloom_tech"
    res = pd.merge(res, pd.read_csv("data/raw_data/bloom_fields.csv"), on = "bloom_field")
    if "bloom_tech" not in nav.get_tables(conn=JPOD_CONN):
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
    ### => here needs to be a saftey need with a try-catch clause similar to insert_base.py (-> need to check if uniq_id-bloom_field identifier is uniq) 
    
    res.to_sql(name = "bloom_tech", con = JPOD_CONN, if_exists="append", index=False)
    JPOD_CONN.commit()
    JPOD_CONN.close()
    print("Successfully inserted information regarding disruptive technology fields by Bloom et al. (2020).")