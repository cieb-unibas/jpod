import sys
from jpod import datagen as dg
from jpod import navigate as nav

#### connect to the database and get its structure -----------------------
DB_DIR = sys.argv[1]
#DB_DIR = "C:/Users/matth/Desktop/"
JPOD_CONN = nav.db_connect(db_path = DB_DIR)
JPOD_STRUCTURE = dg.base_properties()

for table in JPOD_STRUCTURE.tables:
    n_rows = JPOD_CONN.execute("SELECT COUNT(*) FROM {};".format(table)).fetchone()[0]
    if n_rows != 0:
        JPOD_CONN.execute("DELETE FROM {};".format(table))
        print("Deleted {} rows from table '{}'".format(n_rows, table))

#### insert data from raw files of 2022 -----------------------
DAT_DIR = sys.argv[2]
#DAT_DIR = "C:/Users/matth/Desktop/"
FILES = dg.select_raw_files(DAT_DIR)
for file in FILES:
    dat = dg.load_raw_data(DAT_DIR + file)
    print("Data from file '{}' loaded".format(file))

    for table in JPOD_STRUCTURE.tables:
        print("Insert data from raw file '{}' into database table '{}'".format(file, table))
        table_dat = dg.structure_data(
            df = dat, 
            table_vars = JPOD_STRUCTURE.tablevars[table], 
            table_pkey = JPOD_STRUCTURE.pkeys[table],
            lowercase_vars = JPOD_STRUCTURE.str_matching_vars, 
            distinct_postings = True
            )
        
        # insert data into JPOD.db:
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

#### Save and close connection to .db -------------------
JPOD_CONN.commit()
JPOD_CONN.close()