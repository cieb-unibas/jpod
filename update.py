import os
import sqlite3
import pandas as pd

import jpod

#### directories
DB_DIRS = [
    "C:/Users/nigmat01/Desktop/", 
    "C:/Users/matth/Desktop/",
    "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
    ]
DAT_DIRS = [
    "C:/Users/nigmat01/Desktop/", 
    "C:/Users/matth/Documents/github_repos/", 
    "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/jobspickr_raw/jobspickr2023/"
    ]

def get_path(potential_paths):
    dir_out = [p for p in potential_paths if os.path.exists(p)][0]
    return dir_out

def get_jpod_columns(conn):
    jpod_tables = [t for t in jpod.get_tables(conn) if t != "regio_grid"]
    jpod_columns = [v[1] for t in jpod_tables for v in conn.execute("PRAGMA table_info(%s);" % t).fetchall()]
    return jpod_columns

# def load_and_structure(file, conn):
#     df = jpod.load_raw_data(file)
#     df = df.rename(columns = {"inferred_iso3_lang_code": "text_language", "is_remote": "remote_position"})
#     cols = get_jpod_columns(conn)
#     df = df[[c for c in df.columns if c in cols]]
#     return df

# def lowercase_columns(dat, columns):
#     for c in columns:
#         c_notnull = dat[c].notnull()
#         c_lower = dat[c].dropna().astype(str).str.strip().str.lower()
#         dat.loc[c_notnull, c] = c_lower
#     return dat

def load_jpod_nuts(conn):
    # nuts regions and codes
    nuts_query = """
    SELECT name_en AS inferred_state, nuts_2, nuts_3
    FROM regio_grid
    WHERE nuts_level = 2 OR nuts_level = 3;
    """
    regio_nuts = pd.read_sql(con = conn, sql = nuts_query)
    # oecd regions and codes
    oecd_query = """
    SELECT name_en AS inferred_state, oecd_tl2 AS nuts_2, oecd_tl3 AS nuts_3
    FROM regio_grid
    WHERE nuts_2 IS NULL AND nuts_3 IS NULL  AND (oecd_level = 2 OR oecd_level = 3);
    """
    regio_oecd = pd.read_sql(con = conn, sql = oecd_query)
    # combine and return
    regions = pd.concat([regio_nuts, regio_oecd], axis= 0)
    return regions

def get_uniq_ids(df : pd.DataFrame, out_var : str, levels : list = ["inferred_country", "job_description"]):
    uniq = df.groupby(levels).first()["uniq_id"].reset_index(drop = True)
    uniq = pd.DataFrame(uniq)
    uniq[out_var] = "yes"
    print(f"Identified {len(df) - len(uniq)} duplicated postings on indicated level")
    return uniq

def get_bloom_keywords(path: str = "data/bloom_tech.csv"):
    bloom = pd.read_csv(path)
    for v in ["en", "de", "fr", "it"]:
        bloom["keyword_" + v] = [w.replace(r"%", r"@%") for w in bloom["keyword_" + v]]
        bloom["keyword_" + v] = [w.replace(r"_", r"@_") for w in bloom["keyword_" + v]]
        bloom["keyword_" + v] = [w.replace(r"'", r"''") for w in bloom["keyword_" + v]]
    fields_keywords = {}
    for field in set(bloom["bloom_field"]):
        for v in ["en", "de", "fr", "it"]:
            keywords = list(bloom[(bloom.bloom_field) == field]["keyword_" + v])
            keywords = list(set(keywords))
        fields_keywords[field] = keywords
    return fields_keywords

def get_ai_keywords(path: str = "data/acemoglu_ai_keywords.csv"):
    df = pd.read_csv(path)
    for v in ["en", "de", "fr", "it"]:
        df["keyword_" + v] = [w.replace(r"%", r"@%") for w in df["keyword_" + v]]
        df["keyword_" + v] = [w.replace(r"_", r"@_") for w in df["keyword_" + v]]
        df["keyword_" + v] = [w.replace(r"'", r"''") for w in df["keyword_" + v]]
    keywords = []
    for v in ["en", "de", "fr", "it"]:
        keywords += list(df.loc[:, "keyword_" + v])
        keywords = list(set(keywords))
    return keywords

def _add_bloom_codes(df, path = "data/raw_data/bloom_fields.csv"):
    bloom_codes = pd.read_csv("data/raw_data/bloom_fields.csv")
    df = df.merge(bloom_codes, how = "left", on = "bloom_field")
    return df

def _test_drop_cols(df):
    """
    Check if dataframe `df` features column names that are not featured in JPOD
    """
    drop_cols = [c for c in df.columns if c not in get_jpod_columns(JPOD_CONN)]
    return drop_cols


if __name__ == "__main__":
    
    print("---------------Setting directories---------------")
    
    # database
    JPOD_VERSION = "jpod_test.db"
    DATA_BATCH = "jobspickr_2023_01"
    DB_DIR = get_path(DB_DIRS)
    DB_DIR = os.path.join(DB_DIR, JPOD_VERSION)
    JPOD_CONN = sqlite3.connect(DB_DIR)
    JPOD_STRUCTURE = jpod.base_properties()
    
    # raw data
    DAT_DIR = get_path(DAT_DIRS)
    FILES = jpod.select_raw_files(DAT_DIR)  
    
    print("---------------Updating JPOD---------------")
    
    log_n = 20

    # get existing p_keys for unique_records
    pkey_exist = {}
    for table in JPOD_STRUCTURE.tables:
        pkey_exist[table] = jpod.retrieve_pkeys(table = table, p_key = JPOD_STRUCTURE.pkeys[table], conn = JPOD_CONN)

    for file, i in enumerate(FILES):

        df = jpod.load_raw_data(os.path.join(DAT_DIR, file)).rename(columns = {"inferred_iso3_lang_code": "text_language", "is_remote": "remote_position"})
        
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
            assert not _test_drop_cols(df = table_dat)

            # add (default) information not contained in raw data
            if table == "job_postings":
                table_dat["data_batch"] = DATA_BATCH
                table_dat["unique_posting_text"] = "yes" # default value
                table_dat["unique_posting_textlocation"] = "yes" # default value
            elif table == "position_characteristics":
                table_dat = table_dat.merge(load_jpod_nuts(conn=JPOD_CONN), how = "left", on = "inferred_state")
            table_vars = jpod.get_table_vars(conn = JPOD_CONN, table = table)
            assert all(c in table_dat.columns for c in table_vars), "Some columns from JPOD table `%s` are missing in the provided dataframe" % table

            # insert
            try: 
                jpod.insert_base_data(df=table_dat, table = table, conn = JPOD_CONN, test_rows = False)
            except:
                table_dat = jpod.unique_records(df = table_dat, df_identifier = p_key, existing_pkeys = pkey_exist[table])
                jpod.insert_base_data(df=table_dat, table = table, conn = JPOD_CONN, test_rows = False)
            if len(table_dat[p_key]) > 0:
                pkey_exist[table] |= set(table_dat[p_key])
            print("All data from file '{}' successfully processed.".format(file))
            JPOD_CONN.commit()

        # ==> das geht eigentlich viel besser direkt in SQL... hier nur einfügen.
        
        # identify bloom: --------------------------------------------------------------
        keyword_dict = get_bloom_keywords()
        bloom = pd.DataFrame()
        for field in keyword_dict.keys():
            contains_keywords = df["job_description"].map(lambda x: any(keyword in str(x).lower() for keyword in keyword_dict[field]))
            uniq_ids_bloom_field = pd.DataFrame(df[contains_keywords]["uniq_id"])
            if len(uniq_ids_bloom_field) > 0:
                uniq_ids_bloom_field["bloom_field"] = field
                print("Searching for job postings in the field {0} completed. Number of postings retrieved: {1}".format(field, len(uniq_ids_bloom_field)))
            else:
                print("Searching for job postings in the field %s completed. Number of postings retrieved: 0" %field)    
            bloom = pd.concat([bloom, uniq_ids_bloom_field], axis=0)
        bloom = _add_bloom_codes(df=bloom)
        ### => hier braucht es ggf. auch noch eine Sicherheitshürde, um duplicates zu vergindern. Oder aber machen dann alles innerhalb der .db mittels sql...
        ### => bei dg.keyword_query dann ein databatch einbauen, ggf. sogar ein country.
        # FROM (
        #     SELECT uniq_id, lower({1}) as {1}
        #     FROM job_postings
        #     WHERE data_batch='batch' 
        #     ) jp
        # LEFT JOIN position_characteristics pc ON pc.uniq_id = jp.uniq_id => DIES KANN EIGENTLICH WEG UND STANDARDISIERT NUR NOCH UNIQ_ID ABRUFEN MIT DER FUNKTION
        bloom.to_sql(name = "bloom_tech", con = JPOD_CONN, if_exists="append", index=False)
        JPOD_CONN.commit()

        # identify ai -----------------------------------------------------------------------
        keywords = get_ai_keywords()
        contains_keywords = df["job_description"].map(lambda x: any(keyword in str(x).lower() for keyword in keywords))
        uniq_ids_ai = pd.DataFrame(df[contains_keywords]["uniq_id"])
        if len(uniq_ids_ai) > 0:
            print("Searching for job postings with connection to AI completed. Number of postings retrieved: {}".format(len(uniq_ids_ai)))
        else:
            print("Searching for job postings with connection to AI completed. Number of postings retrieved: 0")
        uniq_ids_ai.to_sql(name = "acemoglu_ai", con = JPOD_CONN, if_exists="append", index=False)
        JPOD_CONN.commit()

        # identify duplicates ------------------------------------------------------------------
        # ==> need to check exsting ones
        # uniq_country = get_uniq_ids(df = df, out_var = "unique_posting_text", levels=["inferred_country", "job_description"])
        # uniq_regio = get_uniq_ids(df = df, out_var = "unique_posting_textlocation", levels=["inferred_country", "inferred_state","job_description"])
        # df = df.merge(uniq_country, how="left", on = "uniq_id").merge(uniq_regio, how="left", on = "uniq_id")
        # df[["unique_posting_textlocation", "unique_posting_text"]] = df[["unique_posting_textlocation", "unique_posting_text"]].fillna("no")

        # logs
        if i % log_n == 0 and i != 0:
            print("Inserted data from %d of %d files into JPOD" % (i, len(FILES)))

