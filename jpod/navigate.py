import sqlite3

def db_connect(db_path):
    """
    Connect to JPOD.
    
    Parameters:
    -----------
    db_path : str
        A string indicating the location of JPOD.
    
    Returns:
    --------
    sqlite3.Connection:
        A sqlite3 connection object to JPOD.
    """
    conn = sqlite3.connect(db_path + "jpod.db")
    return conn

def get_tables(conn):
    """
    Retrieve all tables from JPOD

    Parameters:
    -----------
    conn : sqlite3.Connection
        A sqlite3 connection object to JPOD.

    Returns:
    --------
    list :
        A list of strings indicating JPOD tables.
    """
    res = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    res = [col[0] for col in res.fetchall()]
    return res
    
def get_table_vars(conn, table):
    """
    Retriebe all columns from a table in JPOD

    Parameters:
    -----------
    conn : sqlite3.Connection
        A sqlite3 connection object to JPOD.
    table :str
        A string indicating a certain JPOD table.

    Returns:
    --------
    list:
        A list of strings indicating columns of a JPOD table.
    """
    res = conn.execute("PRAGMA table_info({});".format(table))
    res = [col[1] for col in res.fetchall()]
    return res

class base_properties():
    """
    Retrieve the base properties of JPOD.

    Attributes:
    -----------
    tables : list
        A list of strings indicating the base tables in JPOD.
    pkeys : dict
        A dictionary of tables (str) and their respective primary keys (str) in JPOD.
    tablevars : dict
        A dictionary of tables (str) and their respective column names (str) in JPOD
    lowercase_vars : list
        A list of column names in JPOD that are lowercase.
    """
    def __init__(self):
        self.tables = ["job_postings", "position_characteristics", "institutions"]
        self.pkeys = {"job_postings": "uniq_id", "position_characteristics": "uniq_id", "institutions": "company_name"}
        self.tablevars = {
            "job_postings": ["crawl_timestamp", "url", "post_date", "job_title", "job_description",
            "html_job_description", "job_board", "text_language"
            ],
            "position_characteristics": ["company_name", "category", "inferred_department_name", 
            "inferred_department_score", "city", "inferred_city", "state", "inferred_state", 
            "country", "inferred_country", "job_type", "inferred_job_title", "remote_position"
            ],
            "institutions": ["contact_phone_number", "contact_email", "inferred_company_type", "inferred_company_type_score"
            ]}
        self.lowercase_vars = [
            "job_title", "job_board", "company_name", "category", "inferred_department_name",
            "city", "inferred_city", "state", "inferred_state", "country", "inferred_country", 
            "job_type", "inferred_job_title", "inferred_company_type"]

def empty_table(table, conn):
    """
    Delete all existing observations in a JPOD table.

    Parameters:
    ----------
    table : str
        A string representing the JPOD table
    conn : sqlite3.Connection
        A sqlite3 connection object to JPOD.
    """
    n_rows_table = conn.execute("SELECT COUNT(*) FROM {};".format(table)).fetchone()[0]
    if n_rows_table != 0:
        conn.execute("DELETE FROM {};".format(table))
        print("Deleted {} rows from table '{}'".format(n_rows_table, table))
    else:
        print("JPOD table '{}' is already empty.".format(table))
