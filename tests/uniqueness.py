import pandas as pd
import sqlite3

# connect to the databse
DB_DIR = "C:/Users/matth/Desktop/"
DB_VERSION = "jpod_test.db"
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)

# choose a random sample of 50 employers in the database
JPOD_QUERY ="""
SELECT company_name
FROM institutions 
WHERE company_name IN (SELECT company_name FROM institutions ORDER BY RANDOM() LIMIT %d);
""" % 1000
sample_employers = JPOD_CONN.execute(JPOD_QUERY).fetchall()
sample_employers = [e[0] for e in sample_employers]

# 1) total number of postings for each of these companies
JPOD_QUERY ="""
SELECT company_name, COUNT(uniq_id) AS total_postings
FROM position_characteristics 
WHERE company_name IN ({0})
GROUP BY company_name
ORDER BY -total_postings;
""".format(str(sample_employers)[1:-1])
n_postings = pd.read_sql(con = JPOD_CONN, sql = JPOD_QUERY)

# 2) all postings for each of these companies
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
""".format(str(sample_employers)[1:-1])
all_postings = pd.read_sql(con = JPOD_CONN, sql = JPOD_QUERY)
res = all_postings.merge(n_postings, on = "company_name")

# 3) unique number of postings for each of these companies
n_unique_postings = res.groupby(["company_name"])["job_description"].nunique().rename("unique_postings")
res = res.merge(n_unique_postings, on = "company_name")
res["share_unique"] = res["unique_postings"] / res["total_postings"]
res = res.sort_values(["share_unique"]).drop_duplicates(["company_name"]).drop(["uniq_id", "job_description"], axis = 1)
print(res.head(10))
