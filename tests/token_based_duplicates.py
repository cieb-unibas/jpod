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

# set parameters for the test
N_EMPLOYERS = 1000
PERIOD = "full_sample"
MIN_POSTINGS = 2
N_TOKEN_SEQUENCES = 5
TOKEN_SEQUENCE_LENGTH = 7

# retrieve a random sample of employers for a specific period
sample_employers = jpod_tests.get_employer_sample(con = JPOD_CONN, n=N_EMPLOYERS, min_postings=MIN_POSTINGS, months = PERIOD)
n_employers = len(sample_employers)
print("Retrieved %d employers from time period %s" % (n_employers, PERIOD))
sample_employers = [e for e in sample_employers if e != None]
sample_employers = jpod_tests.get_employer_postings(con = JPOD_CONN, employers = sample_employers, months = PERIOD)
print("Retrieved %d postings from %d employers in time period %s" % (len(sample_employers), N_EMPLOYERS, PERIOD))

# find all postings with the exact same postings text
employer_postings = pd.DataFrame(sample_employers.groupby(["company_name"])["company_name"].count().rename("n_postings"))
employer_postings = employer_postings.merge(sample_employers.groupby(["company_name"])["job_description"].nunique().rename("non_identical_postings"), on="company_name")
employer_postings["exact_duplicates"] = employer_postings["n_postings"] - employer_postings["non_identical_postings"]
employer_postings["token_based_duplicates"] = 0

# evaluate duplicates:
for e in sample_employers["company_name"]:
    token_based_duplicates = []
    
    # select all non-duplicated postings by employer e and skip to the next if there is only one posting
    emp_postings = sample_employers[sample_employers["company_name"] == e]
    emp_postings = emp_postings.drop_duplicates(subset="job_description").reset_index().drop("index", axis = 1)
    if len(emp_postings) <= 1:
        continue 

    # for each posting p of employer e, check if there are other postings that share several token-sequences
    for posting_idx, employer_posting in enumerate(emp_postings["job_description"]):
        
        # define the posting uniq_id:
        uid = emp_postings["uniq_id"][posting_idx]

        # if this particular posting has already been identified as a duplicate continue with the next one
        if uid in token_based_duplicates:
            continue

        # extract randomly chosen sequences of tokens from posting p
        token_sequences = jpod_tests.random_token_sequences_from_text(
            text = employer_posting, 
            sequence_length = TOKEN_SEQUENCE_LENGTH, 
            n_sequences = N_TOKEN_SEQUENCES
            )

        # select all other postings from employer e that have not been checked or not been identified as duplicates already:
        remaining_postings = emp_postings.iloc[emp_postings.index.drop(range(posting_idx+1))] # exclude already checked
        remaining_postings = remaining_postings[remaining_postings["uniq_id"].isin(token_based_duplicates) == False] # already labelled as duplicates
        if len(remaining_postings) <= 1:
            continue
        
        # if some of the remaining postings of employer e share the token sequences contained in posting p, 
        # add these postings' uniq_id's to the list of token_based_duplicates
        for uid, text in zip(remaining_postings["uniq_id"], remaining_postings["job_description"]):
            if all(s in text for s in token_sequences):
                token_based_duplicates.append(uid)

    # count token-based duplicates of this firm:
    n_token_based_duplicates = len(token_based_duplicates)
    if len(token_based_duplicates) > 0:
        employer_postings.loc[e, "token_based_duplicates"] = n_token_based_duplicates

# summarize:
employer_postings["unique_postings"] = employer_postings["non_identical_postings"] - employer_postings["token_based_duplicates"]
employer_postings["unique_share"] = employer_postings["unique_postings"] / employer_postings["n_postings"]
n_exact_duplicates = sum(employer_postings["exact_duplicates"])
n_token_based_duplicates = sum(employer_postings["token_based_duplicates"])
employer_postings["n_duplicates"] = employer_postings["exact_duplicates"] + employer_postings["token_based_duplicates"] 
n_duplicates =  n_exact_duplicates + n_token_based_duplicates
n_total = sum(employer_postings["n_postings"])

# print results ----
print("""
    Total number of duplicated postings from employers with at least {0} published posting(s): {1} of {2} ({3}%)
    """.format(MIN_POSTINGS, n_duplicates , n_total, round(100 * n_duplicates / n_total, 2))
    )
print("""
    Total number of exactly duplicated postings from employers with at least {0} published posting(s): {1} of {2} ({3}%)
    """.format(MIN_POSTINGS, n_exact_duplicates , n_total, round(100 * n_exact_duplicates / n_total, 2))
    )
print("""
    Total number of token-based duplicated postings from employers with at least {0} published posting(s): {1} of {2} ({3}%)
    """.format(MIN_POSTINGS, n_token_based_duplicates , n_total, round(100 * n_token_based_duplicates / n_total, 2))
    )
print("""
    Share of token-based methodology duplicates among all detected duplicates from employers with at least {0} published posting(s): {1}%
    """.format(MIN_POSTINGS, round(100 * n_token_based_duplicates / n_duplicates, 2))
    )
print("""
    Share of employers with at least {0} published posting(s) that have duplicates: {1}%
    """.format(MIN_POSTINGS, round(100 * len(employer_postings[employer_postings["n_duplicates"] > 0]) / len(employer_postings), 2))
    )
print("""
    Average number of duplicated postings from employers with at least {0} published posting(s): {1}
    """.format(MIN_POSTINGS, round(employer_postings["n_duplicates"].mean(), 1))
    )
print("""
    Average share of unique postings from employers with at least {0} published posting(s): {1}%
    """.format(MIN_POSTINGS, round(100 * employer_postings["unique_share"].mean(), 2))
    )

print(employer_postings.sort_values("unique_share").head(10).loc[:,["n_postings", "unique_postings", "unique_share"]])

# for testing:
# e = "homeservice24"
# posting_idx = 1
# employer_posting = emp_postings["job_description"][posting_idx]
# emp_postings["job_description"][0]
# [len(x) for x in emp_postings["job_description"]]