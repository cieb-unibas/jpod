import os
import sys
import sqlite3
import time

import pandas as pd

print("Current directory is: " + os.getcwd())
sys.path.append(os.getcwd())

import jpod

def get_country_sample(con, n_postings: int = 200) -> pd.DataFrame:
    query = """
    SELECT uniq_id, company_name 
    FROM (
        SELECT uniq_id, inferred_country, company_name, ROW_NUMBER() OVER(PARTITION BY inferred_country) AS nth_posting
        FROM position_characteristics
        )
    WHERE nth_posting <= %d
    """ % n_postings    
    country_samples = pd.read_sql(sql = query, con=con)
    return country_samples
    
def get_overall_random_sample(con, sample_share: int = 0.002) -> pd.DataFrame:
    n_samples = int(JPOD_CONN.execute("SELECT COUNT(*) FROM position_characteristics").fetchall()[0][0] * sample_share)
    query = """
    SELECT uniq_id, company_name
    FROM position_characteristics
    WHERE uniq_id IN (SELECT uniq_id FROM position_characteristics ORDER BY RANDOM() LIMIT %d);
    """ % n_samples
    samples = pd.read_sql(sql = query, con=con)
    return samples

def insert_into_test_db_table(con, table: str, identifier: str, restrict_to: list[str], commit = True) -> None:
    restriction = ", ".join(["'" + x + "'" for x in restrict_to])
    query = """
    INSERT INTO jpod_test.{0}
    SELECT *
    FROM {0}
    WHERE {1} IN ({2})
    """.format(table, identifier, restriction)
    con.execute(query)
    if commit:
        con.commit()
    print("Data inserted into jpod_test.db table %s" % table)

def get_institutions_table_insert_query():
    query = """
    INSERT INTO jpod_test.institutions
    SELECT int.company_name, int.contact_email, int.contact_phone_number, int.inferred_company_type, int.inferred_company_type_score
    FROM (
        SELECT DISTINCT(company_name)
        FROM jpod_test.position_characteristics
        WHERE company_name IS NOT NULL
        ) jtpc 
    LEFT JOIN institutions int ON jtpc.company_name = int.company_name
    WHERE int.company_name IS NOT NULL
    """
    return query

if __name__ == "__main__":
    start = time.perf_counter()

    # connect to JPOD and attach a new database jpod_test.db
    JPOD_DIR = os.path.join(jpod.get_path(jpod.config.DB_DIRS), "jpod.db")
    JPOD_TEST_DIR = os.path.join(jpod.get_path(jpod.config.DB_DIRS), "jpod_test.db")
    JPOD_CONN = sqlite3.connect(database = JPOD_DIR)

    # create a random sample of job postings ------------------------------------
    overall_sample = get_overall_random_sample(con=JPOD_CONN, sample_share = 0.002).dropna() # 0.2% of overall database
    country_sample = get_country_sample(con = JPOD_CONN, n_postings = 200).dropna() # get maximum 200 examples for every inferred_country
    print("Number of randomly retrieved samples: %d" % len(overall_sample))
    print("Number of retrieved samples to ensure every country is representeted: %d" % len(country_sample))
    sample_uniq_ids = pd.concat([overall_sample, country_sample], axis = 0).drop_duplicates(subset=["uniq_id"])
    sample_uniq_ids = sample_uniq_ids["uniq_id"].to_list()
    print("Defined %d unique samples to insert into jpod_test.db" % len(sample_uniq_ids))

    # attach database jpod_test.db and insert data from the samples in JPOD into it ------------------------------------
    attach_query = "ATTACH DATABASE '%s' AS jpod_test;" % JPOD_TEST_DIR
    JPOD_CONN.execute(attach_query)
    JPOD_CONN.execute("INSERT INTO jpod_test.regio_grid SELECT * FROM regio_grid")
    for table in ["job_postings", "position_characteristics", "bloom_tech", "acemoglu_ai"]:
        insert_into_test_db_table(con = JPOD_CONN, table=table, identifier="uniq_id", restrict_to=sample_uniq_ids)
    JPOD_CONN.execute(get_institutions_table_insert_query())
    for table in jpod.get_tables(conn = JPOD_CONN):
        print("Number of rows in table %s: %d" % (table, len(pd.read_sql("SELECT * FROM %s" % table, con=JPOD_CONN))))
    
    # close .db and log
    JPOD_CONN.commit()
    JPOD_CONN.close()
    end = time.perf_counter()
    print("Inserted all data into jpod_test.db")
    print("Execution took %f minutes." % round((end - start) / 60, ndigits=3))
