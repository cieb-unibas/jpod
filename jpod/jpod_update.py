import sys
import pandas as pd
import navigate as nav
import datagen as dg

#### connect to the database and get its base structure -----------------------
#DB_DIR = sys.argv[1]
DB_DIR = "C:/Users/matth/Desktop/"
JPOD_CONN = nav.db_connect(db_path = DB_DIR)
JPOD_BASE_STRUCTURE = nav.base_properties()

#### get current structure of JPOD
TABLES = nav.get_tables(conn=JPOD_CONN)
TABLES_VARS = {}
for table in TABLES:
    TABLES_VARS[table] = nav.get_table_vars(table = table, conn = JPOD_CONN)
DB_VARS = []
for table in TABLES:
    DB_VARS += TABLES_VARS[table]
DB_VARS = list(set(DB_VARS))

#### Example: Inserting new data:
some_df = pd.DataFrame(
    [["migros"] + ["A" for x in range(6)], ["Bigpet"] + ["b" for x in range(6)]], 
    columns=TABLES_VARS["institutions"] + ["x", "y"]) 

# 1) the last two coluumns will be dropped by the dg.structure_data() method because they are not present in JPOD
# 2) inferred_company_type and "company_name" will be lowercased, since they are defined as potential matching variables
dat_insert = dg.structure_data(
    df = some_df, 
    table_vars = TABLES_VARS["institutions"], 
    table_pkey = JPOD_BASE_STRUCTURE.pkeys["institutions"],
    lowercase_vars = JPOD_BASE_STRUCTURE.str_matching_vars, 
    distinct_postings = True
    )
dat_insert

# 3) Migros will be dropped because this company is already present in JPOD.
dat_insert = dg.unique_data_preparation(df=dat_insert, id="company_name", table="institutions", conn = JPOD_CONN)
dat_insert

# insert data
#dat_insert.to_sql(name = "institutions", con = JPOD_CONN, index = False, if_exists = "append")
