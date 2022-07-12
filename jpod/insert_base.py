import sys
#sys.path.append("./jpod")
import datagen as dg
import navigate as nav

#### connect to the database and get its base structure -----------------------
DB_DIR = sys.argv[1]
#DB_DIR = "C:/Users/matth/Desktop/"
JPOD_CONN = nav.db_connect(db_path = DB_DIR)
JPOD_STRUCTURE = nav.base_properties()

# empty the tables if necessary (for developement purposes)
for table in JPOD_STRUCTURE.tables:
    nav.empty_table(table = table, conn = JPOD_CONN)

#### insert data from JobsPickr -----------------------
DAT_DIR = DB_DIR + "jobspickr_raw/"
#DAT_DIR = DB_DIR # desktop
FILES = dg.select_raw_files(DAT_DIR)


# get existing pkeys in the tables here.
# store them in memory as np.array in order to not connect to JPOD in every loop (at the end, there will be around 3.4Mio. elements in this array..)
# in the loop: 1) check if new records match existing pkeys 2) if not: append the pkey array with the new ones.
def retrieve_pkeys(table, pkey):
    sql_statement = """SELECT %s FROM %s;""" % (pkey, table)
    existing_keys = pd.read_sql_query(sql_statement, con = conn)[pair[1]]
    existing_keys = np.array(existing_keys)
    return existing_keys




for file in FILES:
    dat = dg.load_raw_data(DAT_DIR + file)
    dat = dat.rename(columns = {"inferred_iso3_lang_code": "text_language", "is_remote": "remote_position"})
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
        dg.insert_base_data(df=table_dat, table = table, conn = JPOD_CONN)
    print("All data from file '{}' successfully inserted into JPOD.".format(file))

# summarize
for table in JPOD_STRUCTURE.tables:
    N_obs = JPOD_CONN.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
    print("Number of observations in table '{}':".format(table), N_obs)

#### Save and close connection to .db -------------------
JPOD_CONN.commit()
JPOD_CONN.close()