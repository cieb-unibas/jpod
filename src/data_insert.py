import sys
import sqlite3
import data_gen as dg

#### define the input data files (AUTOMATE THIS FOR BASH)-----------------------
DAT_DIR = sys.argv[1]
FILE_FORMAT = sys.argv[2]
DB_DIR = sys.argv[3]
# DAT_DIR = "C:/Users/matth/Desktop/"
# FILE_FORMAT = ".zip"
# DB_DIR = "C:/Users/matth/Desktop/"

#### select the variables to store for different tables and specify their primary keys -----------------------
TABLES_PKEYS = {"job_postings": "uniq_id", "position_characteristics": "uniq_id", "institutions": "company_name"}
TABLES_VARS = {
    "job_postings": [
        "crawl_timestamp", "url", "post_date", "job_title", "job_description",
        "html_job_description", "job_board", "text_language"
        ],
    "position_characteristics": [
        "company_name", "category", "inferred_department_name", "inferred_department_score", 
        "city", "inferred_city", "state", "inferred_state", "country", "inferred_country", 
        "job_type", "inferred_job_title", "remote_position"
        ],
    "institutions": [
        "contact_phone_number", "contact_email", "inferred_company_type", "inferred_company_type_score"
    ]}

#### establish connection to the JPOD Databes ---------------------
JPOD_CONN = sqlite3.connect(DB_DIR + "jpod.db")
for table in TABLES_PKEYS.keys():
    N_obs_table = JPOD_CONN.execute("SELECT COUNT(*) FROM {};".format(table)).fetchone()[0]
    if N_obs_table != 0:
        JPOD_CONN.execute("DELETE FROM {};".format(table))
        print("Deleted data from table '{}'".format(table))

#### insert data from the files -----------------------
FILES = dg.select_files(DAT_DIR, file_format = FILE_FORMAT)
for file in FILES:
    dat = dg.load_data(DAT_DIR + file).rename(columns = {"inferred_iso3_lang_code": "text_language", "is_remote": "remote_position"})  
    for table in TABLES_PKEYS.keys():
        table_dat = dg.structure_data(
            df = dat, 
            table_vars = TABLES_VARS[table], 
            table_pkey = TABLES_PKEYS[table], 
            distinct_postings = True
            )
        # insert into .db:
        if table == "institutions":
            table_dat = dg.unique_data_preparation(df = table_dat, id = TABLES_PKEYS[table], table = table, conn = JPOD_CONN)
            N_INSERTATIONS = len(table_dat)
            table_dat.to_sql(name = table, con = JPOD_CONN, index = False, if_exists = "append")
            dg.test_data_structure(n_insertations = N_INSERTATIONS, table = table, conn = JPOD_CONN)
        else:
            N_INSERTATIONS = len(table_dat)
            table_dat.to_sql(name = table, con = JPOD_CONN, index = False, if_exists = "append")
            # test:
            dg.test_data_structure(n_insertations = N_INSERTATIONS, table = table, conn = JPOD_CONN)
        print("{} observations from file '{}' for table '{}' successfully inserted into JPOD.".format(N_INSERTATIONS, file, table))

#### summarize ------------------------------
for table in TABLES_VARS.keys():
    N_obs = JPOD_CONN.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
    print("Number of observations in table '{}':".format(table), N_obs)

#### Save and close connection to .db
JPOD_CONN.commit()
JPOD_CONN.close()
