# import sqlalchemy as sqla # see https://docs.sqlalchemy.org/en/14/tutorial/index.html#unified-tutorial for information
# sqla.__version__
import sqlite3
from data_gen import *

#### define the input data files -----------------------
DAT_DIR = "C:/Users/matth/Desktop/"
DB_DIR = "C:/Users/matth/Desktop/"
FILES = select_files(DAT_DIR, file_format = ".zip")

#### select the variables to store for different tables and specify their primary keys -----------------------
TABLES_VARS = {
    "job_postings": [
        "uniq_id", # primary key must be first element
        "crawl_timestamp", "url", "post_date", "job_title", "job_description",
        "html_job_description", "job_board", "text_language"
        ],
    "position_characteristics": [
        "uniq_id", # primary key must be first element
        "company_name", "category", "inferred_department_name", "inferred_department_score", 
        "city", "inferred_city", "state", "inferred_state", "country", "inferred_country", 
        "job_type", "inferred_job_title", "remote_position"
        ],
    "institutions": [
        "company_name", # primary key must be first element
        "contact_phone_number", "contact_email", "inferred_company_type", "inferred_company_type_score"
    ]
    }

TABLES_PKEYS = dict()
for table in TABLES_VARS.keys():
    TABLES_PKEYS[table] = TABLES_VARS[table][0]

#### establish connection to the JPOD Databes ---------------------
# sqla library
# jpod_engine = sqla.create_engine("sqlite+pysqlite:////"+DAT_DIR+"jpod.db", echo=False) # 4 slashes (////) for absolute paths
# jpod_conn = jpod_engine.connect()
# pd.read_sql('select * from job_postings', con = jpod_conn)
# conn.close()
# jpod_engine.dispose()

# open connection using sqlite3 library: ---
jpod_conn = sqlite3.connect(DB_DIR+"jpod.db")
for table in TABLES_PKEYS.keys():
    N_obs_table = jpod_conn.execute("SELECT COUNT(*) FROM job_postings;").fetchone()[0]
    if N_obs_table != 0:
        jpod_conn.execute("DELETE FROM {};".format(table))
        print("Deleted data from table '{}'".format(table))

#### insert the data -----------------------
for file in FILES:
    dat = load_data(DAT_DIR + file)
    dat = dat.rename(columns={"inferred_iso3_lang_code": "text_language", "is_remote": "remote_position"})  
    
    for table in TABLES_VARS.keys():
        table_dat = structure_data(
            df = dat, 
            table_vars = TABLES_VARS[table], 
            table_pkey = TABLES_PKEYS[table], 
            distinct_postings = True
            )
        N_obs_file = len(table_dat) # for testing

        # insert into db:
        table_dat.to_sql(name = table, con = jpod_conn, index = False, if_exists = "append")
        
        # test if observations match
        N_obs_table = jpod_conn.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
        assert N_obs_file == N_obs_table

        print("{} observations from file '{}' for table '{}' successfully inserted into JPOD.".format(N_obs_file, file, table))

# close connection usqing sqlite3 library
for table in TABLES_VARS.keys():
    N_obs = jpod_conn.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
    print("Number of observations in table '{}':".format(table), N_obs)

jpod_conn.commit()
jpod_conn.close()
