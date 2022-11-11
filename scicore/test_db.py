import sqlite3
import sys
import os
print(os.getcwd())
sys.path.append(os.getcwd()+"/jpod/")
import navigate as nav
import datagen as dg

#### connect to JPOD -----------------------
DB_DIR = sys.argv[1]
JPOD_CONN = sqlite3.connect(DB_DIR + "jpod.db")
JPOD_STRUCTURE = nav.base_properties()

#### retrieve samples for creating a random subset:-----------------------
test_size = 0.005 # 0.5% of overall samples
n_samples = int(JPOD_CONN.execute("SELECT COUNT(*) FROM job_postings").fetchall()[0][0] * test_size)
JPOD_QUERY = """
SELECT jp.uniq_id
FROM job_postings jp
WHERE uniq_id IN (SELECT uniq_id FROM job_postings ORDER BY RANDOM() LIMIT %d);
""" % n_samples
samples = JPOD_CONN.execute(JPOD_QUERY).fetchall()
samples = [s[0] for s in samples]
JPOD_CONN.close()

#### connect to jpod_test.db -----------------------
TEST_DB = sqlite3.connect(DB_DIR + "jpod_test.db")
pkey_exist = {} # get a set of existing pkeys per table.
for table in JPOD_STRUCTURE.tables:
    pkey_exist[table] = dg.retrieve_pkeys(table = table, p_key = JPOD_STRUCTURE.pkeys[table], conn = TEST_DB)

#### insert data from JobsPickr -----------------------
#DAT_DIR = DB_DIR
DAT_DIR = DB_DIR + "jobspickr_raw/"
FILES = dg.select_raw_files(DAT_DIR)
for file in FILES:
    dat = dg.load_raw_data(DAT_DIR + file)
    dat = dat.rename(columns = {"inferred_iso3_lang_code": "text_language", "is_remote": "remote_position"})
    dat = dat.loc[(dat.uniq_id.isin(samples)), :]
    print("Data from file '{}' loaded and subsetted".format(file))
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
            dg.insert_base_data(df=table_dat, table = table, conn = TEST_DB, test_rows = False)
        except:
            table_dat = dg.unique_records(df = table_dat, df_identifier = p_key, existing_pkeys = pkey_exist[table])
            dg.insert_base_data(df=table_dat, table = table, conn = TEST_DB, test_rows = False)
        if len(table_dat[p_key]) > 0:
            pkey_exist[table] |= set(table_dat[p_key])
        print("All data from file '{}' successfully processed.".format(file))

# summarize
for table in JPOD_STRUCTURE.tables:
    N_obs = TEST_DB.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
    assert N_obs == len(pkey_exist[table]), "Stored number of uniqe pkey values does not correspond to number of rows in the JPOD table '%s'" % table
    print("Number of observations in table '{}':".format(table), N_obs)
