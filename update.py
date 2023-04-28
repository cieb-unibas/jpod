import sqlite3
import pandas as pd

import jpod

JPOD_VERSION = "jpod_test.db"
JPOD_CONN = sqlite3.connect("C:/Users/matth/Desktop/" + JPOD_VERSION)
JPOD_STRUCTURE = jpod.base_properties()

DAT_DIR = "C:/Users/matth/Documents/github_repos/"
FILES = jpod.select_raw_files(DAT_DIR)

def get_jpod_columns(conn = JPOD_CONN):
    jpod_tables = [t for t in jpod.get_tables(conn) if t != "regio_grid"]
    jpod_columns = [v[1] for t in jpod_tables for v in conn.execute("PRAGMA table_info(%s);" % t).fetchall()]
    return jpod_columns

def load_and_structure(file = DAT_DIR + FILES[0], conn = JPOD_CONN):
    df = jpod.load_raw_data(file)
    df = df.rename(columns = {"inferred_iso3_lang_code": "text_language", "is_remote": "remote_position"})
    cols = get_jpod_columns(conn)
    df = df[[c for c in df.columns if c in cols]]
    return df

def lowercase_columns(dat, columns):
    for c in columns:
        c_notnull = dat[c].notnull()
        c_lower = dat[c].dropna().astype(str).str.strip().str.lower()
        dat.loc[c_notnull, c] = c_lower
    return dat

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

  
def _add_bloom_codes(df, path = "data/raw_data/bloom_fields.csv"):
    bloom_codes = pd.read_csv("data/raw_data/bloom_fields.csv")
    df = df.merge(bloom_codes, how = "left", on = "bloom_field")
    return df




#### load data
df = load_and_structure()
df = lowercase_columns(df, columns = JPOD_STRUCTURE.lowercase_vars)

#### assign regional codes
df = df.merge(load_jpod_nuts(conn=JPOD_CONN), how="left", on = "inferred_state")
df.groupby(["nuts_2", "inferred_state"])[["nuts_2"]].count()

#### indicate duplicate status
uniq_country = get_uniq_ids(df = df, out_var = "unique_posting_text", levels=["inferred_country", "job_description"])
uniq_regio = get_uniq_ids(df = df, out_var = "unique_posting_textlocation", levels=["inferred_country", "inferred_state","job_description"])
df = df.merge(uniq_country, how="left", on = "uniq_id").merge(uniq_regio, how="left", on = "uniq_id")
df[["unique_posting_textlocation", "unique_posting_text"]] = df[["unique_posting_textlocation", "unique_posting_text"]].fillna("no")

# identify bloom
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
bloom.to_sql(name = "bloom_tech", con = JPOD_CONN, if_exists="append", index=False)

# identify ai


# insert to jpod

if __name__ == "__main__":
    
    print("---------------Updating JPOD---------------")
    DAT_DIR = "C:/Users/matth/Documents/github_repos/"
    FILES = jpod.select_raw_files(DAT_DIR)
    log_n = 20
    
    for file, i in enumerate(FILES):

        # load and structure data
        df = load_and_structure(file = DAT_DIR + file, conn=JPOD_CONN)
        df = lowercase_columns(df, columns = JPOD_STRUCTURE.lowercase_vars)

        # assign regional codes to samples
        df = df.merge(load_jpod_nuts(conn=JPOD_CONN), how="left", on = "inferred_state")

        # indicate duplicate status
        uniq_country = get_uniq_ids(df = df, out_var = "unique_posting_text", levels=["inferred_country", "job_description"])
        uniq_regio = get_uniq_ids(df = df, out_var = "unique_posting_textlocation", levels=["inferred_country", "inferred_state","job_description"])
        df = df.merge(uniq_country, how="left", on = "uniq_id").merge(uniq_regio, how="left", on = "uniq_id")
        df[["unique_posting_textlocation", "unique_posting_text"]] = df[["unique_posting_textlocation", "unique_posting_text"]].fillna("no")

        # identify bloom:
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
        bloom.to_sql(name = "bloom_tech", con = JPOD_CONN, if_exists="append", index=False)

        # logs
        if i % log_n == 0:
            print("Inserted data from %d file into JPOD" % i)

