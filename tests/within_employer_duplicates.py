import sys
import pandas as pd
import sqlite3
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

# evaluate within employer duplicates
N = 1000

for c in [1, 2, 3, 5, 10]:
    MIN_POSTINGS = c
    print("Evaluating within employer duplicates for employers with at least %d postings" % MIN_POSTINGS)
    
    sample_employers = jpod_tests.get_employer_postings(con = JPOD_CONN, employers = jpod_tests.get_employer_sample(con = JPOD_CONN, n=N, min_postings=MIN_POSTINGS))
    
    employer_postings = pd.DataFrame(sample_employers.groupby(["company_name"])["company_name"].count().rename("n_postings"))
    employer_postings = employer_postings.merge(sample_employers.groupby(["company_name"])["job_description"].nunique().rename("unique_postings"), on="company_name")
    employer_postings["share_unique_postings"] = employer_postings["unique_postings"] / employer_postings["n_postings"]
    employer_postings["duplicates"] = employer_postings["n_postings"] - employer_postings["unique_postings"]
    n_duplicates = sum(employer_postings["duplicates"])
    n_total = sum(employer_postings["n_postings"])
    
    print("""
        Total number of duplicated postings from employers with at least {0} published posting(s): {1} of {2} ({3}%)
        """.format(MIN_POSTINGS, n_duplicates , n_total, round(100 * n_duplicates / n_total, 2))
        )
    print("""
        Share of employers with at least {0} published posting(s) that have duplicates: {1}%
        """.format(MIN_POSTINGS, round(100 * len(employer_postings[employer_postings["duplicates"] > 0]) / len(employer_postings), 2))
        )
    print("""
        Average number of duplicated postings from employers with at least {0} published posting(s): {1}
        """.format(MIN_POSTINGS, round(employer_postings["duplicates"].mean(), 1))
        )
    print("""
        Average share of unique postings from employers with at least {0} published posting(s): {1}%
        """.format(MIN_POSTINGS, round(100 * employer_postings["share_unique_postings"].mean(), 2))
        )

    print(employer_postings.sort_values("share_unique_postings").head())