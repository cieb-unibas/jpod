import sys
import navigate as nav
import datagen as dg
import pandas as pd

#### connect to JPOD
DB_DIR = sys.argv[1]
JPOD_CONN = nav.db_connect(db_path = DB_DIR)

#### load AI keywords and translations used by Acemoglu et al. (2019)
df = pd.read_csv(DB_DIR + "augmentation_data/acemoglu_ai_keywords.csv")
for v in ["en", "de", "fr", "it"]:
   df["keyword_" + v] = [w.replace(r"%", r"@%") for w in df["keyword_" + v]]
   df["keyword_" + v] = [w.replace(r"_", r"@_") for w in df["keyword_" + v]]
   df["keyword_" + v] = [w.replace(r"'", r"''") for w in df["keyword_" + v]]

#### define keywords and SQL query for the techfield:
keywords = []
for v in ["en", "de", "fr", "it"]:
    keywords += list(df.loc[:, "keyword_" + v])
    keywords = list(set(keywords))
JPOD_QUERY = dg.keyword_query(
    keywords = keywords, 
    matching_column = "job_description", 
    output_variables = ["uniq_id"]
    )
    
#### retrieve and annotate postings with a connection to the techfield:
res = pd.read_sql(con = JPOD_CONN, sql=JPOD_QUERY)
if len(res) > 0:
    print("Searching for job postings with connection to AI completed. Number of postings retrieved: {}".format(len(res)))
else:
    print("Searching for job postings with connection to AI completed. Number of postings retrieved: 0")

#### create new table "acemoglu_ai" and insert data into the table
JPOD_QUERY = """
DROP TABLE IF EXISTS acemoglu_ai;
CREATE TABLE acemoglu_ai(
    uniq_id VARCHAR(32) NOT NULL,
    PRIMARY KEY (uniq_id)
    );
"""
JPOD_CONN.executescript(JPOD_QUERY)
res.to_sql(name = "acemoglu_ai", con = JPOD_CONN, if_exists="append", index=False)

#### Save and close connection to .db -------------------
JPOD_CONN.commit()
JPOD_CONN.close()