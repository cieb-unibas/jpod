import sys
import pandas as pd
import sqlite3

# connect to the databse
DB_DIR = sys.argv[1]
DB_VERSION = sys.argv[2]
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)
JPOD_CONN = sqlite3.connect("C:/Users/nigmat01/Desktop/jpod_test.db")

#################### BETWEEN EMPLOYERS #########################
# choose a random sample of N employers in the database
N = 100
JPOD_QUERY ="""
SELECT company_name
FROM institutions 
WHERE company_name IN (SELECT company_name FROM institutions ORDER BY RANDOM() LIMIT %d);
""" % N
sample_employers = JPOD_CONN.execute(JPOD_QUERY).fetchall()
sample_employers = [e[0] for e in sample_employers]
print("Retrieved number of sample employers: ", len(sample_employers))

# choose a random sample of (N * 1000) postings from other employers
JPOD_QUERY ="""
SELECT uniq_id
FROM job_postings
WHERE uniq_id IN (
    SELECT uniq_id 
    FROM position_characteristics
    WHERE company_name NOT IN ({0})
    )
ORDER BY RANDOM()
LIMIT {1}
""".format(str(sample_employers)[1:-1], N * 1000)
benchmark_postings = JPOD_CONN.execute(JPOD_QUERY).fetchall()
benchmark_postings = [p[0] for p in benchmark_postings]
# JPOD_QUERY="""
# SELECT DISTINCT uniq_id, job_description
# FROM job_postings
# WHERE uniq_id IN ({0})
# """.format(str(benchmark_postings)[1:-1])
JPOD_QUERY="""
SELECT uniq_id, rn
FROM (
    SELECT uniq_id, ROW_NUMBER() OVER(PARTITION BY job_description ORDER BY uniq_id DESC) rn
    FROM job_postings
    WHERE uniq_id IN ({0})
    )
WHERE rn = 1
""".format(str(benchmark_postings)[1:-1])
benchmark_postings = JPOD_CONN.execute(JPOD_QUERY).fetchall()
benchmark_postings = [p[0] for p in benchmark_postings]
print("Retrieved number of unique benchmark postings: ", len(benchmark_postings))

# check for each employer if its postings have counterparts in those of benchmark postings
df = pd.DataFrame()
for e in sample_employers[:10]:
    res = {}
    res["employer"] = e
    res["n_postings"] = JPOD_CONN.execute("SELECT COUNT(*) FROM position_characteristics WHERE company_name = '%s'" % e.replace(r"'", r"''")).fetchone()[0]

    JPOD_QUERY ="""
    SELECT job_description, uniq_id
    FROM job_postings 
    WHERE uniq_id IN (SELECT uniq_id FROM position_characteristics WHERE company_name = '%s')
    """ % e.replace(r"'", r"''")
    employer_postings = JPOD_CONN.execute(JPOD_QUERY).fetchall()
    employer_postings = [p[0] for p in employer_postings]
    res["n_distinct_postings"] = len(set(employer_postings))
    
    # n_counterparts = 0
    # for p in employer_postings:
    #     JPOD_QUERY ="""
    #     SELECT COUNT(*)
    #     FROM job_postings
    #     WHERE uniq_id IN ({0}) AND job_description == '{1}'
    #     """.format(str(benchmark_postings)[1:-1], p.replace(r"'", r"''"))
    #     n_counterparts =+ JPOD_CONN.execute(JPOD_QUERY).fetchone()[0]
    JPOD_QUERY ="""
    SELECT DISTINCT job_description
    --SELECT COUNT(DISTINCT job_description)
    FROM (
        SELECT job_description
        FROM job_postings
        WHERE uniq_id IN (SELECT uniq_id FROM position_characteristics WHERE company_name == '{0}')
        OR uniq_id IN ({1})
        )
    """.format(e.replace(r"'", r"''"), str(benchmark_postings)[1:-1])
    total_distinct = JPOD_CONN.execute(JPOD_QUERY).fetchall()
    # total_distinct = JPOD_CONN.execute(JPOD_QUERY).fetchone()[0]
    res["n_counterparts"] = len(benchmark_postings) + res["n_distinct_postings"] - len(total_distinct)

    res = pd.DataFrame(res, columns=res.keys(), index=range(1))
    df = pd.concat([df, res], axis = 0)

df = df.reset_index().drop("index", axis = 1).sort_values("n_counterparts", ascending= False)
print(df.head(10))
sum(df["n_counterparts"])

# COUNT(DISTINCT job_description) returns not all values...

##################### AMONG EMPLOYERS ###########################

print("Performing tests for uniqueness of job postings:")
# choose a random sample of N employers in the database
N = 1000
JPOD_QUERY ="""
SELECT company_name
FROM institutions 
WHERE company_name IN (SELECT company_name FROM institutions ORDER BY RANDOM() LIMIT %d);
""" % N
sample_employers = JPOD_CONN.execute(JPOD_QUERY).fetchall()
sample_employers = [e[0] for e in sample_employers]
print("Retrieved number of sample employers: ", N)
# Retrieved number of sample employers:  1000

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
print("Number of sample employers with at least two postings: ", len(employers))
# Number of sample employers with at least two postings:  630

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
print("Number of retrieved postings: ", len(all_postings))
# Number of retrieved postings:  26701
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
# Number of duplicated postings from employers with more than 2 published postings: 8099 of 26701 ( 30.33 %)
print(
    "Share of employers with more than 2 published postings that have duplicates:", 
    round(100 * len(res[res["share_unique"] < 1]) / len(res), 2),
    "%"
)
# Share of employers with more than 2 published postings that have duplicates: 51.11 %

print(res.head(20))
#                            company_name  total_postings  unique_postings  share_unique
#15630          resort hotel alex  zermatt             215               13      0.060465
#12354                        finco search             185               15      0.081081
#20995   parkhotel schoenegg s grindelwald              57                5      0.087719
#24709                            sylva ag              11                1      0.090909
#19695                       pixel plus ag              98                9      0.091837
#19203             alterszentrum sunnmatte              35                4      0.114286
#25440                         reviderm ag              15                2      0.133333
#25966                 sportshop karrer ag              14                2      0.142857
#16520                            fabasoft              39                6      0.153846
#11012     per, private equity recruitment             583               92      0.157804
#24961                  anex ingenieure ag              19                3      0.157895
#17371                          expeditors             285               46      0.161404
#22449               coiffeur studio senna               6                1      0.166667
#14838                        incube group              49                9      0.183673
#23141             pixel dairy productions              49                9      0.183673
#25068             waldhauser + hermann ag              36                7      0.194444
#25290       hemmersbach gmbh &amp; co. kg              15                3      0.200000
#91                              nbn media              35                7      0.200000
#24039                      abc connection               5                1      0.200000
#13176  versicherungspartner costanzo gmbh              10                2      0.200000