import sys
import sqlite3
import navigate as nav
import datagen as dg
import pandas as pd

# connect to JPOD
DB_DIR = sys.argv[1]
DB_VERSION = sys.argv[2]
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)

# load techfield specific keywords, refine their translations, indicate SQLITE-wildcard characters and doubble-quote
df = pd.read_csv(DB_DIR + "augmentation_data/bloom_tech.csv")
for v in ["en", "de", "fr", "it"]:
   df["keyword_" + v] = [w.replace(r"%", r"@%") for w in df["keyword_" + v]]
   df["keyword_" + v] = [w.replace(r"_", r"@_") for w in df["keyword_" + v]]
   df["keyword_" + v] = [w.replace(r"'", r"''") for w in df["keyword_" + v]]

# loop over techfields and extract postings that contain their keywords
res = pd.DataFrame()
for field in set(df["bloom_field"]):
    print("Searching job postings in the field of: %s" %field)
    
    # define keywords and SQL query for the techfield:
    keywords = []
    for v in ["en", "de", "fr", "it"]:
        keywords += list(df[(df.bloom_field) == field]["keyword_" + v])
        keywords = list(set(keywords))
    JPOD_QUERY = dg.keyword_query(
        keywords = keywords, 
        matching_column = "job_description", 
        output_variables = ["uniq_id"]
        )
    
    # retrieve and annotate postings with a connection to the techfield:
    tmp = pd.read_sql(con = JPOD_CONN, sql=JPOD_QUERY)
    if len(tmp) > 0:
        tmp["bloom_field"] = field
        print("Searching for job postings in the field {0} completed. Number of postings retrieved: {1}".format(field, len(tmp)))
    else:
        print("Searching for job postings in the field %s completed. Number of postings retrieved: 0" %field)
    
    # add to the final dataset
    res = pd.concat([res, tmp], axis=0)

# create new table "bloom_tech" and insert data into the table
JPOD_QUERY = """
DROP TABLE IF EXISTS bloom_tech;
CREATE TABLE bloom_tech(
uniq_id VARCHAR(32) NOT NULL,
bloom_field VARCHAR(30),
bloom_code INT,
PRIMARY KEY (uniq_id, bloom_code)
);
"""
JPOD_CONN.executescript(JPOD_QUERY)
res = pd.merge(res, pd.read_csv(DB_DIR + "augmentation_data/bloom_fields.csv"), on = "bloom_field")
res.to_sql(name = "bloom_tech", con = JPOD_CONN, if_exists="append", index=False)

#### Save and close connection to .db -------------------
JPOD_CONN.commit()
JPOD_CONN.close()