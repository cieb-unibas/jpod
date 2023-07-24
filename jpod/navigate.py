import sqlite3
import os

def get_path(potential_paths):
    dir_out = [p for p in potential_paths if os.path.exists(p)][0]
    return dir_out

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

def get_table_pkeys(conn, table):
    """
    Retrieve the name of the primary key from a table in JPOD

    Parameters:
    -----------
    conn : sqlite3.Connection
        A sqlite3 connection object to JPOD.
    table :str
        A string indicating a certain JPOD table.

    Returns:
    --------
    list:
        A list of strings indicating primary keys of a JPOD table.
    """
    res = conn.execute("SELECT table_pkey.name FROM PRAGMA table_info({}) AS table_pkey WHERE table_pkey.pk = 1;".format(table))
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


def _jpod_delete_batch_from_table(table: str, data_batch: str = None):
    if not data_batch:
        batch_statement = ""
    else:
        batch_statement = """
        WHERE uniq_id IN (
            SELECT uniq_id
            FROM job_postings
            WHERE data_batch == '%s'
        )
        """ % data_batch
    
    delete_statement = """
    DELETE 
    FROM %s
    %s
    """ % (table, batch_statement)

    return delete_statement


def empty_table(table, conn, data_batch: str = None):
    """
    Delete all existing observations in a JPOD table for a given batch of data.

    Parameters:
    ----------
    table : str
        A string representing the JPOD table
    conn : sqlite3.Connection
        A sqlite3 connection object to JPOD.
    data_batch: str
        A string representing the JPOD data batch
    """
    
    delete_statement = _jpod_delete_batch_from_table(table = table, data_batch = data_batch)
    if data_batch:
        n_rows_table = conn.execute("SELECT COUNT(*) FROM %s WHERE uniq_id IN (SELECT uniq_id FROM job_postings WHERE data_batch = '%s');" % (table, data_batch)).fetchone()[0]
    else:
        n_rows_table = conn.execute("SELECT COUNT(*) FROM %s WHERE uniq_id IN (SELECT uniq_id FROM job_postings" % table).fetchone()[0]
    
    if n_rows_table != 0:
        conn.execute(delete_statement)
        conn.commit()
        if data_batch:
            print("Deleted %d rows from table '%s' and data batch '%s'" % (n_rows_table, table, data_batch))
        else:
            print("Deleted %d rows from table '%s'" % (n_rows_table, table))
    else:
        if data_batch:
            print("JPOD table '%s' for data batch '%s' is already empty." % (table, data_batch))
        else:         
            print("JPOD table '%s' is already empty.".format(table))