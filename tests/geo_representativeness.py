import sys
import pandas as pd
import sqlite3

# connect to the databse
DB_DIR = sys.argv[1]
DB_VERSION = sys.argv[2]
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)

#### Representativeness wrt spatial distribution of employemnt in CH -------------------------------------
print("Performing tests for spatial distribution:")

# get regional distribution of job postings in the database
JPOD_QUERY = """
SELECT COUNT(*) as total_postings, pc.nuts_2, rg.name_en AS Grossregion
FROM position_characteristics pc
LEFT JOIN (
    SELECT nuts_2, name_en
    FROM regio_grid
    WHERE nuts_level = 2
    ) rg on pc.nuts_2 = rg.nuts_2
GROUP BY pc.nuts_2
"""
postings_dist = pd.read_sql_query(JPOD_QUERY, con=JPOD_CONN)
postings_dist["postings_share"] = postings_dist["total_postings"] / sum(postings_dist["total_postings"])
postings_dist = postings_dist.dropna()

# get the distribution of employment in Switzerland for 2Q2022 from BFS at
# https://www.pxweb.bfs.admin.ch/pxweb/de/px-x-0602000000_102/-/px-x-0602000000_102.px/
emp_dist = pd.read_csv("data/raw_data/ch_total_employed_people_02_2022.csv", sep = ";")

# compare the distributions:
dist = postings_dist.merge(emp_dist, on = "nuts_2")
dist["abs_diff"] = dist["postings_share"] - dist["employed_share"]
dist["rel_diff"] = dist["postings_share"] / dist["employed_share"] - 1
print(dist.loc[:, ["nuts_2", "Grossregion", "postings_share", "employed_share", "abs_diff", "rel_diff"]])
#   nuts_2               Grossregion  postings_share  employed_share  abs_diff  rel_diff
# 0   CH01          région lemanique        0.104119        0.192049 -0.087930 -0.457850
# 1   CH02         espace mittelland        0.177448        0.202428 -0.024980 -0.123403
# 2   CH03  northwestern switzerland        0.135140        0.132970  0.002170  0.016320
# 3   CH04                    zurich        0.280118        0.200881  0.079238  0.394451
# 4   CH05       eastern switzerland        0.111307        0.127493 -0.016186 -0.126958
# 5   CH06       central switzerland        0.116161        0.098735  0.017426  0.176490
# 6   CH07                    ticino        0.010185        0.045444 -0.035259 -0.775881


# => Lake Geneva and Ticino are likely strongly underrepresented (thus alos bias possible)
# => Zurich is likely strongly overrepresented
# => Espace Mittelland & Eastern Switzerland somewhat underrepresented, Central Switzerland somewhat overrepresented