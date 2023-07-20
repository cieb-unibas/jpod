import os
import sys
import sqlite3
import time
import pandas as pd

print("Current directory is: " + os.getcwd())
sys.path.append(os.getcwd())

import jpod

def get_country_sample(con, n_postings: int = 200):
    query = """
    SELECT uniq_id, inferred_country, company_name 
    FROM (
        SELECT uniq_id, inferred_country, company_name, ROW_NUMBER() OVER(PARTITION BY inferred_country) AS nth_posting
        FROM position_characteristics
        )
    WHERE nth_posting <= %d
    """ % n_postings    
    country_samples = pd.read_sql(sql = query, con=con)
    return country_samples
    
def get_overall_random_sample(con, sample_share: int = 0.002):
    n_samples = int(JPOD_CONN.execute("SELECT COUNT(*) FROM position_characteristics").fetchall()[0][0] * sample_share)
    query = """
    SELECT uniq_id, company_name
    FROM position_characteristics
    WHERE uniq_id IN (SELECT uniq_id FROM position_characteristics ORDER BY RANDOM() LIMIT %d);
    """ % n_samples
    samples = pd.read_sql(sql = query, con=con)
    return samples

def insert_into_test_db_table(con, table: str, identifier: str, restrict_to: list[str], commit = True):
    restriction = ", ".join(["'" + x + "'" for x in restrict_to])
    query = """
    INSERT INTO jpod_test.{0}
    SELECT *
    FROM {0}
    WHERE {1} IN ({2}) 
    """.format(table, identifier, restriction)
    con.execute(sql = query)
    print("Data inserted into jpod_test.db table %s" % table)
    if commit:
        con.commit()


if __name__ == "__main__":
    start = time.perf_counter()

    # connect to JPOD and attach a new database jpod_test.db
    DB_DIR = os.path.join(jpod.get_path(jpod.config.DB_DIRS), "jpod.db")
    JPOD_CONN = sqlite3.connect(database = DB_DIR)

    # create a random sample of job postings ------------------------------------
    overall_sample = get_overall_random_sample(con=JPOD_CONN, sample_share=0.002)
    print("Number of randomly retrieved samples: %d" % len(overall_sample))
    # find 200 examples for every inferred_country
    country_sample = get_country_sample(con = JPOD_CONN, n_postings = 200)
    country_sample = country_sample.loc[:,["uniq_id", "company_name"]]
    print("Number of retrieved samples to ensure every country has some samples: %d" % len(country_sample))
    # combine them:
    sample_uniq_ids = pd.concat([overall_sample, country_sample], axis = 0).drop_duplicates(subset=["uniq_id"])
    sample_employers = list(set(sample_uniq_ids["company_name"].to_list()))
    print("Defined %d unique employers to insert into jpod_test.db" % len(sample_employers))
    sample_uniq_ids = sample_uniq_ids["uniq_id"].to_list()
    print("Defined %d unique samples to insert into jpod_test.db" % len(sample_uniq_ids))

    # attach database jpod_test.db and insert data from the samples into it ------------------------------------
    JPOD_CONN.execute(sql="ATTACH DATABASE 'jpod_test.db' AS jpod_test;")
    JPOD_CONN.execute(sql ="INSERT INTO jpod_test.regio_grid SELECT * FROM regio_grid") # regio_grid
    insert_into_test_db_table(con = JPOD_CONN, table="institutions", identifier="company_name", restrict_to=sample_employers)
    for table in ["job_postings", "position_characteristics", "bloom_tech", "acemoglu_ai"]:
        insert_into_test_db_table(con = JPOD_CONN, table=table, identifier="uniq_id", restrict_to=sample_uniq_ids)
    print("Inserted all data into jpod_test.db")
    
    # commit and close
    JPOD_CONN.commit()
    JPOD_CONN.close()
    end = time.perf_counter()
    print("Execution took %f minutes." % round((end - start) / 60, ndigits=3))    