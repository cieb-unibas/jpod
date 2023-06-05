import sys
import sqlite3
import datagen as dg
import navigate as nav

#### connect to the database and get its base structure -----------------------
DB_DIR = sys.argv[1]
JPOD_CONN = sqlite3.connect(DB_DIR + "jpod.db")
JPOD_STRUCTURE = nav.base_properties()

#### insert data from JobsPickr -----------------------
DAT_DIR = DB_DIR + "jobspickr_raw/"
FILES = dg.select_raw_files(DAT_DIR)

# get a set of existing pkeys per table.
pkey_exist = {}
for table in JPOD_STRUCTURE.tables:
    pkey_exist[table] = dg.retrieve_pkeys(table = table, p_key = JPOD_STRUCTURE.pkeys[table], conn = JPOD_CONN)

for file in FILES:
    dat = dg.load_raw_data(DAT_DIR + file)
    dat = dat.rename(columns = {"inferred_iso3_lang_code": "text_language", "is_remote": "remote_position"})
    print("Data from file '{}' loaded".format(file))
    for table in JPOD_STRUCTURE.tables:
        print("Insert data from raw file '{}' into database table '{}'".format(file, table))
        p_key = JPOD_STRUCTURE.pkeys[table]
        table_dat = dg.structure_data(
            df = dat, 
            table_vars = JPOD_STRUCTURE.tablevars[table], 
            table_pkey = p_key,
            lowercase = JPOD_STRUCTURE.lowercase_vars, 
            distinct = True
            )
        try: 
            dg.insert_base_data(df=table_dat, table = table, conn = JPOD_CONN, test_rows = False)
        except:
            table_dat = dg.unique_records(df = table_dat, df_identifier = p_key, existing_pkeys = pkey_exist[table])
            dg.insert_base_data(df=table_dat, table = table, conn = JPOD_CONN, test_rows = False)
        if len(table_dat[p_key]) > 0:
            pkey_exist[table] |= set(table_dat[p_key])
    print("All data from file '{}' successfully processed.".format(file))

# add batch information:
JPOD_CONN.execute("ALTER TABLE job_postings ADD COLUMN data_batch VARCHAR DEFAULT 'jobspickr_2022_01'")

# summarize
for table in JPOD_STRUCTURE.tables:
    N_obs = JPOD_CONN.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
    assert N_obs == len(pkey_exist[table]), "Stored number of uniqe pkey values does not correspond to number of rows in the JPOD table '%s'" % table
    print("Number of observations in table '{}':".format(table), N_obs)

#### Save and close connection to .db -------------------
JPOD_CONN.commit()
JPOD_CONN.close()