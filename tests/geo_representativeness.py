import sys
import pandas as pd
import sqlite3
from tests import jpod_tests
from matplotlib import pyplot as plt

# connect to the databse
DB_DIR = sys.argv[1]
DB_VERSION = sys.argv[2]
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)
# JPOD_CONN = sqlite3.connect("C:/Users/nigmat01/Desktop/jpod_test.db")
# JPOD_CONN = sqlite3.connect("C:/Users/matth/Desktop/jpod_test.db")

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

# get the distribution of vacancies in Switzerland for 2Q2022 from BFS at
# https://www.bfs.admin.ch/bfs/de/home/statistiken/industrie-dienstleistungen/unternehmen-beschaeftigte/beschaeftigungsstatistik/offene-stellen.assetdetail.23748689.html
vac_dist = pd.read_csv("data/raw_data/ch_total_vacancies_02_2022.csv", sep = ";")

# compare the distributions:
dist = pd.merge(emp_dist, vac_dist, on = "nuts_2")
dist = dist.merge(postings_dist, on = "nuts_2")
print(dist.loc[:, ["nuts_2", "Grossregion", "postings_share", "employed_share", "vacancy_share"]])
#   nuts_2               Grossregion  postings_share  employed_share  vacancy_share
# 0   CH01          région lemanique        0.103264        0.192049       0.145449
# 1   CH02         espace mittelland        0.177068        0.202428       0.192696
# 2   CH03  northwestern switzerland        0.137581        0.132970       0.153582
# 3   CH04                    zurich        0.280082        0.200881       0.241906
# 4   CH05       eastern switzerland        0.109990        0.127493       0.130216
# 5   CH06       central switzerland        0.112544        0.098735       0.119674
# 6   CH07                    ticino        0.010899        0.045444       0.016475
# => Lake Geneva and Ticino are likely strongly underrepresented (thus also bias possible)
# => Zurich is likely strongly overrepresented
# => Espace Mittelland & Eastern Switzerland somewhat underrepresented, Central Switzerland somewhat overrepresented

# plot:
plot_df = pd.DataFrame()
for x in ["postings", "employed", "vacancy"]:
    tmp = dist.loc[:, ["nuts_2", "Grossregion", x+"_share"]].rename(columns={x+"_share": "share"})
    tmp["measure"] = x
    plot_df = pd.concat([plot_df, tmp])
jpod_tests.plot_dist(df = plot_df, timewindow = "measure", group = "Grossregion", outcome="share")
plt.savefig("tests/img/representativeness.png")
