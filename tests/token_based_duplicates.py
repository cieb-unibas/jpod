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
print("Setting the following parameters")
N_EMPLOYERS = 1000
PERIOD = "full_sample"
CLEAN_DUPLICATES = True
MIN_POSTINGS = 2
N_TOKEN_SEQUENCES = 3
TOKEN_SEQUENCE_LENGTH = 10
print("""
    Number of employers: {0}; 
    Period: {1};
    Only unique postings = {2}
    Minimal Number of Postings: {3}; 
    Number of sequences: {4};
    Number of tokens per sequence: {5}
    """.format(N_EMPLOYERS, PERIOD, CLEAN_DUPLICATES, MIN_POSTINGS, N_TOKEN_SEQUENCES, TOKEN_SEQUENCE_LENGTH)
    )
# => try different combinations for number of sequences and sequence length and check if they are always the same postings


# retrieve a random sample of employers for a specific period
sample_employers = jpod_tests.get_employer_sample(
    con = JPOD_CONN, n=N_EMPLOYERS, 
    min_postings=MIN_POSTINGS, months = PERIOD
    )
n_employers = len(sample_employers)
print("Retrieved %d employers from time period %s" % (n_employers, PERIOD))
sample_employers = [e for e in sample_employers if e != None]
employer_postings = jpod_tests.get_employer_postings(
    con = JPOD_CONN, employers = sample_employers, 
    months = PERIOD, cleaned_duplicates=CLEAN_DUPLICATES)
print("Retrieved %d postings from %d employers in time period %s" % (len(employer_postings), N_EMPLOYERS, PERIOD))

# find all postings with the exact same postings text
employer_evaluation = pd.DataFrame(employer_postings.groupby(["company_name"])["company_name"].count().rename("n_postings"))
employer_evaluation = employer_evaluation.merge(employer_postings.groupby(["company_name"])["job_description"].nunique().rename("non_identical_postings"), on="company_name")
employer_evaluation["exact_duplicates"] = employer_evaluation["n_postings"] - employer_evaluation["non_identical_postings"]
employer_evaluation["token_based_duplicates"] = 0

# evaluate duplicates:
# for testing:
# e = "coop"
# posting_idx = 0
# employer_posting = emp_postings["job_description"][posting_idx]
# emp_postings["job_description"][0]
# [len(x) for x in emp_postings["job_description"]]

emp_counter = 0
for e in sample_employers:
    
    # keep count of progress:
    emp_counter += 1
    if emp_counter in [50 * x for x in range(N_EMPLOYERS)]:
        print("Proceeding search for %dth employer" % emp_counter)
    
    # placeholder for identified duplicates of employer e
    token_based_duplicates = []
    
    # select all non-duplicated postings by employer e and skip to the next if there is only one posting
    emp_postings = employer_postings[employer_postings["company_name"] == e]
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
            n_sequences = N_TOKEN_SEQUENCES,
            seq_multiple = 5
            )

        # select all other postings from employer e that have not been checked or not been identified as duplicates already:
        remaining_postings = emp_postings.iloc[emp_postings.index.drop(range(posting_idx+1))] # exclude already checked postings
        remaining_postings = remaining_postings[remaining_postings["uniq_id"].isin(token_based_duplicates) == False] # exclude already labelled as duplicate postings
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
        employer_evaluation.loc[e, "token_based_duplicates"] = n_token_based_duplicates

# summarize:
employer_evaluation["unique_postings"] = employer_evaluation["non_identical_postings"] - employer_evaluation["token_based_duplicates"]
employer_evaluation["unique_share"] = employer_evaluation["unique_postings"] / employer_evaluation["n_postings"]
n_exact_duplicates = sum(employer_evaluation["exact_duplicates"])
n_token_based_duplicates = sum(employer_evaluation["token_based_duplicates"])
employer_evaluation["n_duplicates"] = employer_evaluation["exact_duplicates"] + employer_evaluation["token_based_duplicates"] 
n_duplicates =  n_exact_duplicates + n_token_based_duplicates
n_total = sum(employer_evaluation["n_postings"])

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
    """.format(MIN_POSTINGS, round(100 * len(employer_evaluation[employer_evaluation["n_duplicates"] > 0]) / len(employer_evaluation), 2))
    )
print("""
    Average number of duplicated postings from employers with at least {0} published posting(s): {1}
    """.format(MIN_POSTINGS, round(employer_evaluation["n_duplicates"].mean(), 1))
    )
print("""
    Average share of unique postings from employers with at least {0} published posting(s): {1}%
    """.format(MIN_POSTINGS, round(100 * employer_evaluation["unique_share"].mean(), 2))
    )
print(employer_evaluation.sort_values("unique_share").head(10).loc[:,["n_postings", "unique_postings", "unique_share"]])