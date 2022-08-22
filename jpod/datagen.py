import pandas as pd
import numpy as np
import zipfile as zf
import os

def select_raw_files(dir, file_format = ".zip"):
    """Return files of a given format from a directory.
    
    Parameters
    ----------
    dir : str
        A string indicating the location of the files to read.
    file_format: str, optional
        A string indicating the format of the files to be read (default is '.zip').
    
    Returns:
    --------
    list:
        A list of strings indicating files to be read.
    """
    files = os.listdir(dir)
    files = [file for file in files if file.endswith(file_format)]
    return files

def load_raw_data(file):
    """Read zipped or raw .csv file to a pandas DataFrame
    
    Parameters
    ----------
    file : str
        A string indicating the location of the file to read.
    
    Raises:
    ------
    ValueError:
        If `file` is not a zipped .csv or raw .csv file.

    Returns:
    --------
    pd.DataFrame:
        A pd.DataFrame containing the raw data to be inserted in JPOD.
    """
    if file.endswith(".zip"):
        file = zf.ZipFile(file)
        file_list = file.infolist()
        assert len(file_list) == 1
        file_name = file_list[0].filename
        df = pd.read_csv(file.open(file_name), low_memory=False)
    elif file.endswith(".csv"):
        df = pd.read_csv(file)
    else:
        raise ValueError("Data must be provided in raw or zipped .csv format.")
    return df

def structure_data(df, table_vars, table_pkey, lowercase = None, distinct = True):
    """
    Structures and processes the data before inserting it to JPOD.

    Parameters
    ----------
    df : pd.DataFrame
        A pandas DataFrame containing the data
    table_vars : list
        A list of strings that represent JPOD-columns the data will be inserted in.
    table_pkey : str
        A string indicating the primary key in the JPOD-table the data will be inserted in.
    lowercase : list, optional
        A list of strings indicating columns of the JPOD-table that have to be lowercase (default is 'None').
    distinct : bool, optional
        A flag indicating if only observations with a non-duplicated primary key should be considered (default is True).

    Returns
    -------
    pd.DataFrame :
        A pandas DataFrame only consisting of columns that are present in the corresponding JPOD table.
    """
    table_vars = list(set(table_vars + [table_pkey]))
    df = df.loc[:, table_vars]
    if lowercase is not None:
        lowercase_vars = [v for v in lowercase if v in table_vars]
        for v in lowercase_vars:
            v_notnull = df[v].notnull()
            v_lower = df[v].dropna().astype(str).str.strip().str.lower()
            df.loc[v_notnull, v] = v_lower
    if distinct:
        df = df.drop_duplicates(subset = [table_pkey]).reset_index().drop("index", axis = 1)
    df = df.dropna(subset = [table_pkey])
    return df

def retrieve_pkeys(table, p_key, conn):
    """
    Retrieve all the values of the primary key column in a JPOD table.

    Parameters:
    -----------
    table : str
        A string indicating the JPOD table.
    conn : sqlite3.Connection
        A sqlite Connection to JPOD.
    
    Raises:
    -------
    AssertionError
        If `table` is not a JPOD table.
    
    Returns:
    --------
    set :
        A set of all the primary key values of this table.
    """
    sql_statement = """SELECT %s FROM %s;""" % (p_key, table)
    existing_keys = conn.execute(sql_statement).fetchall()
    existing_keys = set([x[0] for x in existing_keys])
    return existing_keys

def unique_records(df, df_identifier, existing_pkeys):
    """
    Subset data to observations with identifiers that are not present as primary key values in JPOD.

    Parameters
    -----------
    df : pd.DataFrame
        A pd.DataFrame containing data to be inserted to JPOD.
    df_identifier : str
        A string indicating a column of `df` that identifies unique rows.
    existing_pkeys : set, list
        A list or set of values that are already in use as primary key values in JPOD.

    Raises:
    -------
    AssertionError:
        If `df_identifier` is not a column of `df`.

    Returns
    -------
    pd.DataFrame :
        A pd.DataFrame that only contains observations with primary keys not present in the JPOD table.
    """
    assert df_identifier in df.columns, "Stated identifier could not be found in the DataFrame."
    new_insertations = np.setdiff1d(list(df[df_identifier]), list(existing_pkeys)) # numpy solution 
    # new_insertations = list(set(df[df_identifier]).difference(existing_pkeys)) # set() difference solution
    if len(new_insertations) > 0:
        df = df.set_index(df_identifier)
        df = df.loc[new_insertations, :].reset_index().drop_duplicates(subset = [df_identifier]).dropna(subset=[df_identifier])
    else:
        df = pd.DataFrame(columns=df.columns)
    return df

def insert_base_data(df, table, conn, test_rows = False):
    """
    Insert data into JPOD.

    Parameters:
    ----------
    df : pd.DataFrame
        A pd.DataFrame containing data to be inserted to JPOD.
    table : str
        A string indicating the JPOD-table the data will be inserted in.
    conn : sqlite3.Connection
        A sqlite Connection to JPOD.
    test_rows : bool, optional
        A flag indicating if there should be a test that all rows from `df` have been inserted to JPOD (default is False).
    """
    if len(df) == 0:
        print("No data to insert into JPOD table '{}'.".format(table))
    elif test_rows:
        projected_insertations = len(df)
        rows_pre = conn.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
        df.to_sql(name = table, con = conn, index = False, if_exists = "append")
        rows_post = conn.execute("SELECT COUNT(*) FROM {}".format(table)).fetchone()[0]
        assert rows_post - rows_pre == projected_insertations, "Number of rows in the original dataframe does not correspond to the number of inserted rows to the database table"
        print("Data insertion tested and successfully added into JPOD table '{}'.".format(table))
    else:
        df.to_sql(name = table, con = conn, index = False, if_exists = "append")
        print("Data inserted into JPOD table '{}'.".format(table))


def sql_like_statement(keywords, matching_column, escape_expression):
    """
    Create a SQL LIKE statement for keyword-search in a matching variable. 

    Parameters:
    ----------
    keywords : list
        A list of keywords to search in the matching variable.
    matching_variable: str
        A string specifiying the JPOD column to be searched for the keywords.
    escape_expression: str
        A string indicating that wildcard characters in SQL ('%', '_') are matched with their literal values. 

    Returns:
    --------
    str:
        A string in a SQL LIKE Statement format.
    """
    keywords = ["'% " + w +"%'" for w in keywords]
    if len(keywords) > 1:
        match_string = " OR {} LIKE ".format(matching_column)
        like_statement = match_string.join(keywords)
        like_statement = matching_column + " LIKE " + like_statement + " ESCAPE '%s'" %escape_expression
    else:
        like_statement = str(matching_column) + " LIKE " + keywords[0] + " ESCAPE '%s'" %escape_expression
    return like_statement

def keyword_query(keywords, matching_column, output_variables, escape_expression = "@"):
    """
    Create a SQL query for a keyword search in a matching variable and define output variables from the position characteristics table. 

    Parameters:
    ----------
    keywords : list
        A list of keywords to search in the matching variable.
    matching_variable: str
        A string specifiying the JPOD column to be searched for the keywords.
    escape_expression: str
        A string indicating an expression that is placed in front of SQLITE wildcard characters ('%', '_') 
        to evaluate them based on their literal values. The default is '@'.

    Returns:
    --------
    str:
        A string in a SQL query format.
    """
    if isinstance(keywords, str):
        keywords = [keywords]
    if isinstance(output_variables, str):
        output_variables = [output_variables]
    
    output_variables = ", ".join(["pc." + var for var in output_variables])
    like_statement = sql_like_statement(
        keywords = keywords, 
        matching_column = matching_column, 
        escape_expression = escape_expression
        )

    JPOD_QUERY = """
    SELECT {0}
    FROM (
        SELECT *
        FROM (
            SELECT uniq_id, lower({1}) as {1}
            FROM job_postings
            ) jp
        WHERE ({2})
        ) jp
    LEFT JOIN position_characteristics pc ON pc.uniq_id = jp.uniq_id
    """.format(output_variables, matching_column,like_statement)

    return JPOD_QUERY





