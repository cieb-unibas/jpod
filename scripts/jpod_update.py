import os
import sqlite3
import sys
import time

import pandas as pd

print("Current directory is: " + os.getcwd())
sys.path.append(os.getcwd())

import jpod

if __name__ == "__main__":
    
    start = time.perf_counter()
    
    print("---------------Setting directories---------------")
    JPOD_VERSION = "jpod.db"
    DATA_BATCH = "jobspickr_2023_01"
    DB_DIR = os.path.join(jpod.get_path(jpod.config.DB_DIRS), JPOD_VERSION)

    AUGMENT_PATH = jpod.get_path(jpod.config.DAT_DIRS)    
    JPOD_CONN = sqlite3.connect(database = DB_DIR)
    JPOD_STRUCTURE = jpod.base_properties()

    FILES = jpod.select_raw_files(dir = jpod.get_path(jpod.config.DAT_DIRS))
    if os.path.exists("/scicore/home/weder/GROUP/Innovation/05_job_adds_data/not_inserted_files.csv"):
        not_inserted_files = pd.read_csv("/scicore/home/weder/GROUP/Innovation/05_job_adds_data/not_inserted_files.csv")["files"].to_list()
        FILES = [file for file in FILES if file in not_inserted_files]
    UID_DUPLICATES = pd.read_csv(os.path.join(AUGMENT_PATH, "duplicated_ids.csv"))["duplicated_ids"].to_list()
    log_n = 20
    
    # get existing p_keys for unique_records check
    pkey_exist = {}
    for table in JPOD_STRUCTURE.tables:
        pkey_exist[table] = jpod.retrieve_pkeys(table = table, p_key = JPOD_STRUCTURE.pkeys[table], conn = JPOD_CONN)
    # get existing table variables
    jpod_table_vars = {}
    for table in jpod.get_tables(JPOD_CONN):
        if table == "regio_grid":
            continue
        jpod_table_vars[table] = jpod.get_table_vars(JPOD_CONN, table)
    jpod_cols = [] 
    for cols in jpod_table_vars.values():
        jpod_cols += cols

    print("---------------Updating JPOD with Data Batch '%s'---------------" % DATA_BATCH)

    for i, file in enumerate(FILES):

        df = jpod.load_raw_data(os.path.join(jpod.get_path(jpod.config.DAT_DIRS), file))\
            .rename(columns = {"inferred_iso3_lang_code": "text_language", "is_remote": "remote_position"})
       
        for table in JPOD_STRUCTURE.tables:
            print("Insert data from raw file '{}' into database table '{}'".format(file, table))
            p_key = JPOD_STRUCTURE.pkeys[table]
            table_dat = jpod.structure_data(
                df = df, 
                table_vars = JPOD_STRUCTURE.tablevars[table], 
                table_pkey = p_key,
                lowercase = JPOD_STRUCTURE.lowercase_vars, 
                distinct = True
                )
            assert not [c for c in table_dat.columns if c not in jpod_cols]

            # add (default) information not contained in raw data
            if table == "job_postings":
                table_dat["data_batch"] = DATA_BATCH
                table_dat["unique_posting_text"] = "yes" # default value
                table_dat["unique_posting_textlocation"] = "yes" # default value
            elif table == "position_characteristics":
                table_dat = table_dat.merge(jpod.load_jpod_nuts(conn=JPOD_CONN), how = "left", on = "inferred_state")
            assert all(c in table_dat.columns for c in jpod_table_vars[table]), "Some columns from JPOD table `%s` are missing in the provided dataframe" % table

            # insert the data into the databse
            try:
                keep_ids = [uid for uid in table_dat[p_key] if uid not in UID_DUPLICATES]
                table_dat = table_dat.loc[table_dat.uniq_id.isin(keep_ids), :]
                jpod.insert_base_data(df = table_dat, table = table, conn = JPOD_CONN, test_rows = False)
            except:
                table_dat = jpod.unique_records(df = table_dat, df_identifier = p_key, existing_pkeys = pkey_exist[table])
                jpod.insert_base_data(df=table_dat, table = table, conn = JPOD_CONN, test_rows = False)
            if len(table_dat[p_key]) > 0:
                pkey_exist[table] |= set(table_dat[p_key])
        
        # logs & commit
        if i % log_n == 0 and i != 0:
            print("Inserted data from %d/%d files into JPOD. Approximately %d postings inserted" % (i, len(FILES), i * 100000))
        JPOD_CONN.commit()
    
    print("Data from all files successfully inserted.")
    end = time.perf_counter()
    print("Execution took %f minutes." % round((end - start) / 60, ndigits=3))
