import sys
from jpod import navigate as nav

#### connect to the database and get its structure -----------------------
DB_DIR = sys.argv[1]
#DB_DIR = "C:/Users/matth/Desktop/"
JPOD_CONN = nav.db_connect(db_path = DB_DIR)

TABLES = nav.get_tables(conn=JPOD_CONN)

TABLES_VARS = {}
for table in TABLES:
    TABLES_VARS[table] = nav.get_table_vars(table = table, conn = JPOD_CONN)

DB_VARS = []
for table in TABLES:
    DB_VARS += TABLES_VARS[table]
DB_VARS = list(set(DB_VARS))