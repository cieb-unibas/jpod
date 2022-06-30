import sqlite3

def db_connect(db_path):
    conn = sqlite3.connect(db_path + "jpod.db")
    return conn

def get_tables(conn):
    res = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    res = [col[0] for col in res.fetchall()]
    return res
    
def get_table_vars(conn, table):
    res = conn.execute("PRAGMA table_info({});".format(table))
    res = [col[1] for col in res.fetchall()]
    return res

