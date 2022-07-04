####### DEVELOPMENT ONLY. THIS WILL BE UPDATED WHEN DATA FROM NEW SOURCES SHOULD BE AVAILABLE #######

import sys
import pandas as pd
import navigate as nav
import datagen as dg

#### connect to the database and get its base structure -----------------------
DB_DIR = sys.argv[1]
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
    [["migros"] + ["A" for x in range(6)], ["Bigpet"] + ["B" for x in range(6)]], 
    columns=TABLES_VARS["institutions"] + ["x", "y"]) 

# prepare the df:
# 1) the last two coluumns will be dropped by the dg.structure_data() method because they are not present in JPOD
# 2) 'inferred_company_type' and "company_name" will be lowercased, since they are defined as lowercase variables (see e.g. 'base_properties')
# 3) 'Migros' will be dropped because this company is already present in JPOD.
dat_insert = dg.structure_data(
    df = some_df, 
    table_vars = TABLES_VARS["institutions"], 
    table_pkey = JPOD_BASE_STRUCTURE.pkeys["institutions"],
    lowercase = JPOD_BASE_STRUCTURE.lowercase_vars, 
    distinct = True
    )
dat_insert = dg.unique_records(df=dat_insert, id="company_name", table="institutions", conn = JPOD_CONN)
dat_insert

# insert data
#dat_insert.to_sql(name = "institutions", con = JPOD_CONN, index = False, if_exists = "append")

## for creating new variables for "uniq_id" if we have other sources
# def create_id(chars = string.ascii_lowercase + string.digits):
#     id = ''.join(random.choice(chars) for x in range(32)) # 'uniq_id' is a 32-character long string.
#     return id

# def create_identifiers(n_ids, conn, table, id)
#     existing_ids = conn.execute("SELECT {} FROM {}".format(id, table)).fetchall()
#     existing_ids = np.array(existing_ids, dtype=str)
