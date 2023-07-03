import os
import sqlite3
import sys
import time
import csv

print("Current directory is: " + os.getcwd())
sys.path.append(os.getcwd())

import jpod

def _test_drop_cols(df, con):
    """
    Check if dataframe `df` features column names that are not featured in JPOD
    """
    jpod_tables = [t for t in jpod.get_tables(con) if t != "regio_grid"]
    jpod_columns = [v[1] for t in jpod_tables for v in con.execute("PRAGMA table_info(%s);" % t).fetchall()]    
    drop_cols = [c for c in df.columns if c not in jpod_columns]
    return drop_cols

if __name__ == "__main__":
    
    start = time.perf_counter()
    
    print("---------------Setting directories---------------")
    # database
    JPOD_VERSION = "jpod.db"
    DATA_BATCH = "jobspickr_2023_01"

    DB_DIR = os.path.join(jpod.get_path(jpod.config.DB_DIRS), JPOD_VERSION)
    AUGMENT_PATH = jpod.get_path(jpod.config.DAT_DIRS)
    JPOD_CONN = sqlite3.connect(database = DB_DIR)
    JPOD_STRUCTURE = jpod.base_properties()

    # data & parameters
    FILES = jpod.select_raw_files(dir = jpod.get_path(jpod.config.DAT_DIRS)) ## !!! check here if this creates problems when having a new .csv file. alternatively save as .txt!!!
    UID_DUPLICATES = pd.read_csv(AUGMENT_PATH + "duplicated_uniq_ids.csv")
    log_n = 20
    
    # get existing p_keys for unique_records
    pkey_exist = {}
    for table in JPOD_STRUCTURE.tables:
        pkey_exist[table] = jpod.retrieve_pkeys(table = table, p_key = JPOD_STRUCTURE.pkeys[table], conn = JPOD_CONN)

    print("---------------Updating JPOD with Data Batch '%s'---------------" % DATA_BATCH)

    for i, file in enumerate(FILES):

        df = jpod.load_raw_data(os.path.join(jpod.get_path(jpod.config.DAT_DIRS), file))\
            .rename(columns = {"inferred_iso3_lang_code": "text_language", "is_remote": "remote_position"})

        #### make a check here and continute to the next file if it does not pass the test.
        # Idea: check for 5 random uniq_ids from that file if they are already in the database (subset based on city)
        #       if they are all there, then skip to the next file.

        
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
            assert not _test_drop_cols(df = table_dat, con = JPOD_CONN) ### => make this earlier so that not every time a sqlite command has to be performed

            # add (default) information not contained in raw data
            if table == "job_postings":
                table_dat["data_batch"] = DATA_BATCH
                table_dat["unique_posting_text"] = "yes" # default value
                table_dat["unique_posting_textlocation"] = "yes" # default value
            elif table == "position_characteristics":
                table_dat = table_dat.merge(jpod.load_jpod_nuts(conn=JPOD_CONN), how = "left", on = "inferred_state")
            table_vars = jpod.get_table_vars(conn = JPOD_CONN, table = table) ### remove from loop so that this has only to be performed once and saved in a dict.
            assert all(c in table_dat.columns for c in table_vars), "Some columns from JPOD table `%s` are missing in the provided dataframe" % table

            # insert the data into the databse
            try:
                keep_ids = [uid for uid in table_dat[p_key] if uid not in UID_DUPLICATES]
                table_dat = table_dat.loc[table_dat.uniq_id.isin(keep_ids), :]
                jpod.insert_base_data(df = table_dat, table = table, conn = JPOD_CONN, test_rows = False)
            except:
                table_dat = jpod.unique_records(df = table_dat, df_identifier = p_key, existing_pkeys = pkey_exist[table]) ### also introduce a batch condition here
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
