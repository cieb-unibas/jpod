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
N_EMPLOYERS = 10
PERIOD = "full_sample"
MIN_POSTINGS = 2
N_TOKEN_SEQUENCES = 5
TOKEN_SEQUENCE_LENGTH = 10

# retrieve a random sample of employers for a specific period
sample_employers = jpod_tests.get_employer_sample(con = JPOD_CONN, n=N, min_postings=MIN_POSTINGS, months = PERIOD)
n_employers = len(sample_employers)
print("Retrieved %d employers from time period %s" % (N_EMPLOYERS, PERIOD))
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

# for testing:
# e = "hornbach"
# posting_idx = 6
# employer_posting = emp_postings["job_description"][posting_idx]
# emp_postings["job_description"][7]
# [len(x) for x in emp_postings["job_description"]]