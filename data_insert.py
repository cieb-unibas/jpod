import sys
import data_gen as dg

#### connect to the database and get its structure -----------------------
#DB_DIR = sys.argv[1]
DB_DIR = "C:/Users/matth/Desktop/"
JPOD_CONN = dg.jpod_connect(db_path = DB_DIR)
JPOD_STRUCTURE = dg.jpod_properties()
for table in JPOD_STRUCTURE.tables:
    n_rows = JPOD_CONN.execute("SELECT COUNT(*) FROM {};".format(table)).fetchone()[0]
    if n_rows != 0:
        JPOD_CONN.execute("DELETE FROM {};".format(table))
        print("Deleted {} rows from table '{}'".format(n_rows, table))

#### insert data from raw files of 2022 -----------------------
# DAT_DIR = sys.argv[2]
# FILE_FORMAT = sys.argv[3]
DAT_DIR = "C:/Users/matth/Desktop/"
FILE_FORMAT = ".zip"
FILES = dg.select_files(DAT_DIR, file_format = FILE_FORMAT)
for file in FILES:
    dat = dg.load_data(DAT_DIR + file).rename(columns = {"inferred_iso3_lang_code": "text_language", "is_remote": "remote_position"})
    print("File {} loaded".format(file))  
    for table in JPOD_STRUCTURE.tables:
        print("Insert data from raw file '{}' into database table '{}'".format(file, table))
        table_dat = dg.structure_data(
            df = dat, 
            table_vars = JPOD_STRUCTURE.tablevars[table], 
            table_pkey = JPOD_STRUCTURE.pkeys[table], 
            distinct_postings = True
            )
        # insert into .db:
        if table == "institutions":
            table_dat = dg.unique_data_preparation(df = table_dat, id = JPOD_STRUCTURE.pkeys[table], table = table, conn = JPOD_CONN)
            N_INSERTATIONS = len(table_dat)
            table_dat.to_sql(name = table, con = JPOD_CONN, index = False, if_exists = "append")
            dg.test_data_structure(n_insertations = N_INSERTATIONS, table = table, conn = JPOD_CONN)
        else:
            N_INSERTATIONS = len(table_dat)
            table_dat.to_sql(name = table, con = JPOD_CONN, index = False, if_exists = "append")
            # test:
            dg.test_data_structure(n_insertations = N_INSERTATIONS, table = table, conn = JPOD_CONN)
        print("{} observations from file '{}' for table '{}' successfully inserted into JPOD.".format(N_INSERTATIONS, file, table))

# summarize
for table in JPOD_STRUCTURE.tables:
    N_obs = JPOD_CONN.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
    print("Number of observations in table '{}':".format(table), N_obs)

# tests
myquery = """
SELECT company_name 
FROM institutions 
WHERE 
    company_name LIKE "%epfl%"
LIMIT 20;
"""
[x[0] for x in JPOD_CONN.execute(myquery).fetchall()]

myquery = """
SELECT COUNT(*) 
FROM position_characteristics 
WHERE 
    company_name LIKE "%epfl%"
"""
JPOD_CONN.execute(myquery).fetchone()[0]



#### Save and close connection to .db -------------------
JPOD_CONN.commit()
JPOD_CONN.close()

