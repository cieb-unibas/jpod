### find none-value in the institutions table for cleaning:
import sys
import sqlite3

# connect to the databse
DB_DIR = sys.argv[1]
DB_VERSION = sys.argv[2]
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)
# JPOD_CONN = sqlite3.connect("C:/Users/nigmat01/Desktop/jpod_test.db")
# JPOD_CONN = sqlite3.connect("C:/Users/matth/Desktop/jpod_test.db")

# get all employers from the institutions job postings table the same way as in the test:
JPOD_QUERY="""
SELECT uniq_id, company_name
FROM(
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY company_name ORDER BY uniq_id) as rnr
    FROM position_characteristics
    )
WHERE rnr = 1
"""
employers = JPOD_CONN.execute(JPOD_QUERY).fetchall()

# check
n_dictinct_companies = JPOD_CONN.execute("SELECT COUNT(DISTINCT(company_name)) FROM position_characteristics").fetchone()[0]
#assert len(employers) == n_dictinct_companies, "Retrieved number of companies does not match the number retrieved by DISTINCT."

# find the one that is None
none_employer = [x for x in employers if x[1] == None]
assert len(none_employer) == 2, "No employer registered with a 'None' entry found."

# check the corresponding entry from JPOD
JPOD_QUERY = """
SELECT pc.uniq_id, pc.company_name, jp.job_description
FROM (
    SELECT uniq_id, company_name
    FROM position_characteristics
    WHERE uniq_id == '%s'
    ) pc
LEFT JOIN job_postings jp on pc.uniq_id = jp.uniq_id
""" % none_employer[0][0]
none_employer = JPOD_CONN.execute(JPOD_QUERY).fetchall()
print(none_employer)