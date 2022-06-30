import data_gen as dg

#### connect to the database and get its structure -----------------------
#DB_DIR = sys.argv[1]
DB_DIR = "C:/Users/matth/Desktop/"
JPOD_CONN = dg.jpod_connect(db_path = DB_DIR)
JPOD_STRUCTURE = dg.jpod_properties()

# number of job adds by url
myquery = """
SELECT COUNT(*) 
FROM job_postings 
GROUPBY job_board
"""
JPOD_CONN.execute(myquery).fetchone()[0]



# get the number of job adds from the city of Basel
myquery = """
SELECT COUNT(city) 
FROM position_characteristics 
WHERE 
    city LIKE "basel"
"""
JPOD_CONN.execute(myquery).fetchone()[0]

# get job adds from migros in the city of basel
myquery = """
SELECT *
FROM 
    SELECT uniq_id, city, company_name
    FROM 
    position_characteristics AS pc
    WHERE 
        pc.city LIKE "basel"
    LEFT JOIN job_postings 
LIMIT 20;
"""
[x[0] for x in JPOD_CONN.execute(myquery).fetchall()]


#### close connection to .db -------------------
JPOD_CONN.close()

