import pandas as pd
import sqlite3

# Establish a connection to jpod
JPOD_PATH = "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/jpod_test.db"
JPOD_CONN = sqlite3.connect(JPOD_PATH)

# EXAMPLE 1: define a query that returns the number of job adds by job board
jpod_query = """
SELECT job_board as plattform, COUNT(*) as n_postings 
FROM job_postings 
GROUP BY job_board
ORDER BY n_postings DESC;
"""
# Option 1: read via pandas read_sql_query() method:
pd.read_sql_query(jpod_query, con=JPOD_CONN) 
# Opion 2: read via sqlite execute() method
pd.DataFrame(JPOD_CONN.execute(jpod_query).fetchall(), columns=["plattform", "n_postings"]) 

# EXAMPLE 2: Biggest 10 cities with their total number of postings
jpod_query = """
SELECT city, COUNT(city) n_postings
FROM position_characteristics 
GROUP BY city
HAVING city IS NOT "nan"
ORDER BY n_postings DESC
LIMIT 10;
"""
pd.read_sql_query(jpod_query, con=JPOD_CONN)

# EXAMPLE 3: Job positions from a specific company (novartis) in a given city (basel)
jpod_query = """
SELECT company_name, city, inferred_job_title AS job_title
FROM position_characteristics
WHERE city LIKE "basel" AND company_name LIKE "%novartis%"
"""
pd.read_sql_query(jpod_query, con=JPOD_CONN)

# EXAMPLE 4: Job postings and company information using the term "machine learning"
KEYWORD = "machine learning"
jpod_query = """
SELECT pc.company_name, pc.city, jp.job_title, jp.job_description
FROM (
    SELECT uniq_id, job_title, job_description
    FROM job_postings
    WHERE job_description LIKE "%{}%"
    ) jp
LEFT JOIN position_characteristics pc ON pc.uniq_id = jp.uniq_id;
""".format(KEYWORD)
print("Employers asking about '{}': ".format(KEYWORD))
pd.read_sql_query(jpod_query, con=JPOD_CONN)

# EXAMPLE 5: Top 20 companies in terms of the number of postings using the term "machine learning"
KEYWORD = "machine learning"
jpod_query = """
SELECT pc.company_name, COUNT(*) as n_postings
FROM (
    SELECT uniq_id, job_title, job_description
    FROM job_postings
    WHERE job_description LIKE "%{}%"
    ) jp
LEFT JOIN position_characteristics pc ON pc.uniq_id = jp.uniq_id
GROUP BY pc.company_name
ORDER BY n_postings DESC
LIMIT 20;
""".format(KEYWORD)
print("Employers asking about '{}': ".format(KEYWORD))
pd.read_sql_query(jpod_query, con=JPOD_CONN)