import sys
sys.path.append("./jpod")
import datagen as dg
import navigate as nav

#### connect to the database and get its base structure -----------------------
#DB_DIR = sys.argv[1]
DB_DIR = "C:/Users/matth/Desktop/"
JPOD_CONN = nav.db_connect(db_path = DB_DIR)
JPOD_STRUCTURE = nav.base_properties()

# empty the tables if necessary (for developement purposes)
for table in JPOD_STRUCTURE.tables:
    nav.empty_table(table = table, conn = JPOD_CONN)

#### insert data from JobsPickr -----------------------
DAT_DIR = DB_DIR # desktop
#DAT_DIR = DB_DIR + "jobspickr_raw/"
FILES = dg.select_raw_files(DAT_DIR)

for file in FILES[:10]: # for testing only 10 files
    dat = dg.load_raw_data(DAT_DIR + file)
    print("Data from file '{}' loaded".format(file))
    for table in JPOD_STRUCTURE.tables:
        print("Insert data from raw file '{}' into database table '{}'".format(file, table))
        table_dat = dg.structure_data(
            df = dat, 
            table_vars = JPOD_STRUCTURE.tablevars[table], 
            table_pkey = JPOD_STRUCTURE.pkeys[table],
            lowercase = JPOD_STRUCTURE.lowercase_vars, 
            distinct = True
            )
        table_dat = dg.unique_records(df = table_dat, id = JPOD_STRUCTURE.pkeys[table], table = table, conn = JPOD_CONN)
        dg.insert_base_data(table_dat=table_dat, table = table, conn = JPOD_CONN)
    print("All data from file '{}' successfully inserted into JPOD.".format(file))

# summarize
for table in JPOD_STRUCTURE.tables:
    N_obs = JPOD_CONN.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
    print("Number of observations in table '{}':".format(table), N_obs)

#### Save and close connection to .db -------------------
JPOD_CONN.commit()
JPOD_CONN.close()