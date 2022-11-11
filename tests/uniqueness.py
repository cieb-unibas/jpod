import sys
import pandas as pd
import sqlite3

# connect to the databse
DB_DIR = sys.argv[1]
DB_VERSION = sys.argv[2]
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)

print("Performing tests for uniqueness of job postings:")

# choose a random sample of 50 employers in the database
N = 1000
JPOD_QUERY ="""
SELECT company_name
FROM institutions 
WHERE company_name IN (SELECT company_name FROM institutions ORDER BY RANDOM() LIMIT %d);
""" % N
sample_employers = JPOD_CONN.execute(JPOD_QUERY).fetchall()
sample_employers = [e[0] for e in sample_employers]

# total number of postings for each of these N companies
JPOD_QUERY ="""
SELECT company_name, COUNT(uniq_id) AS total_postings
FROM position_characteristics 
WHERE company_name IN ({0})
GROUP BY company_name
ORDER BY -total_postings;
""".format(str(sample_employers)[1:-1])
n_postings = pd.read_sql(con = JPOD_CONN, sql = JPOD_QUERY)
n_postings = n_postings[n_postings["total_postings"] > 1]
employers = [e for e in list(n_postings["company_name"])]

# 2) get all postings for each of these N companies
JPOD_QUERY ="""
SELECT jp.uniq_id, pc.company_name, jp.job_description
FROM job_postings jp
LEFT JOIN (
    SELECT pc.company_name, pc.uniq_id
    FROM position_characteristics pc
    ) pc on pc.uniq_id = jp.uniq_id
WHERE jp.uniq_id IN (
    SELECT uniq_id 
    FROM position_characteristics 
    WHERE company_name IN ({0})
    )
""".format(str(employers)[1:-1])
all_postings = pd.read_sql(con = JPOD_CONN, sql = JPOD_QUERY)
res = all_postings.merge(n_postings, on = "company_name")

# 3) unique number of postings for each of these N companies
n_unique_postings = res.groupby(["company_name"])["job_description"].nunique().rename("unique_postings")
res = res.merge(n_unique_postings, on = "company_name")
res["share_unique"] = res["unique_postings"] / res["total_postings"]
res = res.sort_values(["share_unique"]).drop_duplicates(["company_name"]).drop(["uniq_id", "job_description"], axis = 1)
n_duplicates = sum(res["total_postings"]) - sum(res["unique_postings"])
n_total = sum(res["total_postings"])
print(
    "Number of duplicated postings from employers with more than 2 published postings:", n_duplicates,
    "of", n_total, "(", round(100 * n_duplicates / n_total, 2), "%)" 
)
print(
    "Share of employers with more than 2 published postings that have duplicates:", 
    round(100 * len(res[res["share_unique"] < 1]) / len(res), 2),
    "%"
)
print(res.head(20))
