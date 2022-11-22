import sqlite3
import sys
import random
import pandas as pd
try:
    import jpod_tests
except:
    from tests import jpod_tests

# connect to the databse
DB_DIR = sys.argv[1]
DB_VERSION = sys.argv[2]
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)
# JPOD_CONN = sqlite3.connect("C:/Users/nigmat01/Desktop/jpod_test.db")
# JPOD_CONN = sqlite3.connect("C:/Users/matth/Desktop/jpod_test.db")

# set parameters:
N_POSTINGS = 1000000
N_SAMPLE_EMPLOYERS = 1000
FULL_SAMPLE = True
PERIODS = {}

if FULL_SAMPLE:
    PERIODS["full_sample"] = "full_sample"
else:
    for m in ["2020-12", "2021-05", "2021-10"]:
        PERIODS[m] = jpod_tests.get_timewindow(month = m, month_window = 1)

for p in PERIODS:
    print("Performing test for period:", PERIODS[p])
    print(
        "Searching for identical postings registered under other employers for", 
        N_SAMPLE_EMPLOYERS, "employers within this period."
        )

    # retrieve all postings for period PERIOD
    if PERIODS[p] == "full_sample":
      where_clause = ""
    else:
      where_clause = "WHERE (SUBSTR(crawl_timestamp, 1, 7) IN (%s))" % str(PERIODS[p])[1:-1]
    
    JPOD_QUERY = """
    SELECT pc.company_name, jp.uniq_id, jp.job_description
    FROM (
        SELECT uniq_id, job_description
        FROM job_postings
        %s
        ) jp
    LEFT JOIN (SELECT uniq_id, company_name FROM position_characteristics) pc on pc.uniq_id = jp.uniq_id
    """ % where_clause
    period_postings = pd.read_sql(JPOD_QUERY, con = JPOD_CONN)

    # generate a random subsample from these postings
    if N_POSTINGS > len(period_postings):
        N_POSTINGS = len(period_postings)
    print("Performing serach among", N_POSTINGS, "postings within this period.")
    period_postings = period_postings.iloc[random.sample(range(len(period_postings)), N_POSTINGS)]

    # retrieve a sample of employers to evaluate from these postings
    employers = set(list(period_postings["company_name"]))
    employers = random.sample(list(employers), N_SAMPLE_EMPLOYERS)

    # for every employer look for matches regarding their postings in other employers' postings
    df = pd.DataFrame()
    for e in employers:
        tmp = {}
        tmp["employer"] = e
        # get all unique postings from employer e in that time_window
        tmp["n_postings"] = len(period_postings[period_postings["company_name"] == e])
        e_uniq_postings = period_postings.loc[period_postings["company_name"] == e,:].drop_duplicates(["job_description"])
        tmp["n_unique_postings"] = len(e_uniq_postings)
        
        # get all unique postings from employers other than e in that time window and search for matches
        non_e_postings = period_postings[period_postings["company_name"] != e]["job_description"].drop_duplicates()
        tmp["n_counterparts"] = sum(e_uniq_postings["job_description"].isin(non_e_postings))
        # e_uniq_postings["counterparts"] = e_uniq_postings["job_description"].isin(non_e_postings) # for retrieving all

        # add to resulting data.frame
        tmp = pd.DataFrame(tmp, columns=tmp.keys(), index=range(1))
        df = pd.concat([df, tmp], axis = 0).sort_values("n_counterparts", ascending=False)

    print(df.head(20))
    print("Number of employers with postings that match those of other employers:", sum(df["n_counterparts"]>0))
    print("Share of employers with postings that match those of other employers:", sum(df["n_counterparts"]>0) / N_SAMPLE_EMPLOYERS)
    print("Number of identical postings found among employers:", sum(df["n_counterparts"]))
    print("Share of postings with a counterpart in other employers' postings:", sum(df["n_counterparts"]) / sum(df["n_unique_postings"]))