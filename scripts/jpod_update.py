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
    
    print("---------------Setting parameters---------------")
    JPOD_VERSION = "jpod.db"
    DATA_BATCH = jpod.config.BATCH_VERSION
    DB_DIR = os.path.join(jpod.get_path(jpod.config.DB_DIRS), JPOD_VERSION)

    DATA_DIR = jpod.get_path(jpod.config.DAT_DIRS) + DATA_BATCH    
    JPOD_CONN = sqlite3.connect(database = DB_DIR)

    JPOD_STRUCTURE = jpod.base_properties()
    STATUS_FILE = os.path.join(DB_DIR, "augmentation_data/not_inserted_files.csv")

    FILES = jpod.select_raw_files(dir = DATA_DIR)

    if os.path.exists(STATUS_FILE):
        not_inserted_files = pd.read_csv(STATUS_FILE)["files"].to_list()
        FILES = [file for file in FILES if file in not_inserted_files]
    print("Selected %d files with raw data for updating JPOD" % len(FILES))
    
    UID_DUPLICATES = pd.read_csv(os.path.join(DATA_DIR, "duplicated_ids.csv"))["duplicated_ids"].to_list()
    existing_employers = list(jpod.retrieve_pkeys(table = "institutions", p_key = JPOD_STRUCTURE.pkeys["institutions"], conn = JPOD_CONN))
    
    jpod_table_vars = {}
    for table in jpod.get_tables(JPOD_CONN):
        if table == "regio_grid":
            continue
        jpod_table_vars[table] = jpod.get_table_vars(JPOD_CONN, table)
    
    jpod_cols = [] 
    for cols in jpod_table_vars.values():
        jpod_cols += cols

    record_counter = 0
    log_n = 20
    
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
            if table == "institutions":
                keep_ids = [e for e in table_dat[p_key] if e not in existing_employers] # check which employers are already in the .db table
                if len(keep_ids) > 0:
                    existing_employers += keep_ids # update the list with the new ones
                    table_dat = table_dat.loc[table_dat.company_name.isin(keep_ids), :] # subset to those employers that are not yet in the .db table
                    jpod.insert_base_data(df = table_dat, table = table, conn = JPOD_CONN, test_rows = False) # insert these employers
            else:
                keep_ids = [uid for uid in table_dat[p_key] if uid not in UID_DUPLICATES] # check if there are any duplicate uniq_ids
                table_dat = table_dat.loc[table_dat.uniq_id.isin(keep_ids), :] # subset data to non-duplicated ones
                jpod.insert_base_data(df = table_dat, table = table, conn = JPOD_CONN, test_rows = False) # insert all into .db tables
                if table == "job_postings":
                    record_counter += len(table_dat)
        
        # logs & commit
        if i % log_n == 0 and i != 0:
            print("Inserted data from %d/%d files into JPOD. %d postings inserted" % (i, len(FILES), record_counter))
        JPOD_CONN.commit()
    
    JPOD_CONN.close()
    print("Data from all files successfully inserted.")
    end = time.perf_counter()
    print("Execution took %f minutes." % round((end - start) / 60, ndigits=3))
