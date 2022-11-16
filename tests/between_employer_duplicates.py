import sqlite3
import sys
import random
import pandas as pd

# connect to the databse
DB_DIR = sys.argv[1]
DB_VERSION = sys.argv[2]
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)
# JPOD_CONN = sqlite3.connect("C:/Users/nigmat01/Desktop/jpod_test.db")
# JPOD_CONN = sqlite3.connect("C:/Users/matth/Desktop/jpod_test.db")

def eval_timewindow(month):
    mo = int(month[-2:])
    year = int(month[:4])

    mo_prev = mo - 1
    mo_next = mo + 1
    year_prev = year
    year_next = year
    
    if mo_prev == 0:
        mo_prev = 12
        year_prev = year - 1
    if mo_next == 13:
        mo_next = 1
        year_next = year + 1
    
    months = [mo_prev, mo, mo_next]
    months = ["0"+ str(m) if m < 10 else str(m) for m in months]
    years = [year_prev, year, year_next]
    periods = list(zip(years, months))
    periods = [str(p[0]) + "-" + p[1] for p in periods]
    return periods

# retrieve all postings for period PERIOD
PERIOD = eval_timewindow("2021-05")
JPOD_QUERY = """
SELECT pc.company_name, jp.uniq_id, jp.job_description
FROM (
    SELECT uniq_id, job_description
    FROM job_postings
    WHERE (SUBSTR(crawl_timestamp, 1, 7) IN (%s))
    ) jp
LEFT JOIN (SELECT uniq_id, company_name FROM position_characteristics) pc on pc.uniq_id = jp.uniq_id
""" % str(PERIOD)[1:-1]
period_postings = pd.read_sql(JPOD_QUERY, con = JPOD_CONN)

# generate a random subsample of these postings
N_POSTINGS = 100000
if N_POSTINGS > len(period_postings):
    N_POSTINGS = len(period_postings)
period_postings = period_postings.iloc[random.sample(range(len(period_postings)), N_POSTINGS)]

# retrieve a sample of employers to evaluate
N_SAMPLE_EMPLOYERS = 200
employers = set(list(period_postings["company_name"]))
employers = random.sample(employers, N_SAMPLE_EMPLOYERS)

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
print("Number of postings with a counterpart in other employers' postings:", sum(df["n_counterparts"]))
print("Share of postings with a counterpart in other employers' postings:", sum(df["n_counterparts"]) / sum(df["n_unique_postings"]))