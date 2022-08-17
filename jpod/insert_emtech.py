import sys
#sys.path.append("./jpod")
import navigate as nav
import datagen as dg
import pandas as pd

# connect to JPOD
DB_DIR = sys.argv[1]
#DB_DIR = "C:/Users/matth/Desktop/"
#DB_DIR = "C:/Users/nigmat01/Desktop/"
JPOD_CONN = nav.db_connect(db_path = DB_DIR)

# load techfield specific keywords
df = pd.read_csv(DB_DIR + "bloom_tech.csv")

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
    
    # retrieve an annotate postings with a connection to the techfield:
    tmp = pd.read_sql(con = JPOD_CONN, sql=JPOD_QUERY)
    if len(tmp) > 0:
        tmp["bloom_field"] = field
        print("Searching for job postings in the field {0} completed. Number of postings retrieved: {1}".format(field, len(tmp)))
    else:
        print("Searching for job postings in the field %s completed. Number of postings retrieved: 0" %field)
    
    # add to the final dataset
    res = pd.concat([res, tmp], axis=0)

# create new table "bloom tech" and insert data into the table
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
res = pd.merge(res, pd.read_csv(DB_DIR + "bloom_fields.csv"), on = "bloom_field")
res.to_sql(name = "bloom_tech", con = JPOD_CONN, if_exists="append", index=False)

# EXAMPLE: Number of postings with connection to technologies from Bloom et al. (2021):
JPOD_QUERY = """
SELECT pc.nuts_2, COUNT(DISTINCT(pc.uniq_id)) AS N_postings_bloom
FROM position_characteristics pc
WHERE pc.uniq_id IN (SELECT uniq_id FROM bloom_tech)
GROUP BY nuts_2
ORDER BY N_postings_bloom DESC;
"""
print("EXAMPLE: Number of postings with connection to technologies from Bloom et al. (2021):")
print(pd.read_sql(con = JPOD_CONN, sql = JPOD_QUERY))