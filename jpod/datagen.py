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
    p_key: str, list[str]
        A string or a list of strings indicating the p_keys.
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
    if isinstance(p_key, list):
        p_key_statement = ", ".join(p_key)
        sql_statement = """SELECT %s FROM %s;""" % (p_key_statement, table)
        existing_keys = pd.read_sql(sql_statement, con=conn)
    else:
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

def geo_query(insert_table, insert_variable, matching_variable):
    """
    Updates an existing table with a new column specifying geographical information (NUTS level) of job postings, by matching their canton or city information to NUTS data.  

    Parameters:
    ----------
    insert_table : str
        A string specifiying the table, where a new column that indicates geo labels should be inserted.
    insert_variable: str
        A string specifiying the name of a new JPOD column indicating geographical information.
    matching_variable: str
        A string indicating a JPOD column in the table `insert_table` that is used to match against regions' names from the `regio_grid` table (normally 'state' or 'inferred state').

    Returns:
    --------
    str:
        A string in a SQL format to update the table `insert_table`.
    """
    JPOD_QUERY = """
    -- Check for matches with English region expression 
    UPDATE {0}
    SET {1} = rg.{1}
    FROM regio_grid rg
    WHERE {2} = rg.name_en AND {0}.{1} IS NULL;

    -- Check for matches with German region expression 
    UPDATE {0}
    SET {1} = rg.{1}
    FROM regio_grid rg
    WHERE {2} = rg.name_de AND {0}.{1} IS NULL;

    -- Check for matches with French region expression 
    UPDATE {0}
    SET {1} = rg.{1}
    FROM regio_grid rg
    WHERE {2} = rg.name_fr AND {0}.{1} IS NULL;
    """.format(insert_table, insert_variable, matching_variable)
    return JPOD_QUERY

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

def _generate_sql_batch_condition(data_batch: str = "all"):
    """
    Create a SQL IN statement for applying a sql query to a batched subsample of data. 

    Parameters:
    ----------
    data_batch : str, list[str]
        A list of batches to subset the data. Will be matched to the `data_batch` column in JPOD job_postings table.

    Returns:
    --------
    str:
        A string in a SQL LIKE Statement format.
    """
    if data_batch == "all":
        data_batch_statement = ""
    elif isinstance(data_batch, list):
        data_batch_statement = " OR ".join(["data_batch == '" + batch +"'" for batch in data_batch])
    elif isinstance(data_batch, str):
        data_batch_statement = "data_batch == '" + data_batch + "'"
    return data_batch_statement    

def _generate_sql_country_condition(countries):
    """
    Create a SQL IN statement for applying a sql query to a country subsample of data. 

    Parameters:
    ----------
    countries : str, list[str]
        A list of countries to subset the data. Will be matched to the `inferred_country` column in JPOD job_postings table.

    Returns:
    --------
    str:
        A string in a SQL LIKE Statement format.
    """
    if countries == "all":
        country_condition = ""
    elif isinstance(countries, str):
        countries = [countries]
        country_condition = "inferred_country IN ('" + "', '".join(countries) + "')"
    elif isinstance(countries, list):
        country_condition = "inferred_country IN ('" + "', '".join(countries) + "')"
    return country_condition

def _combine_sql_conditions(condition_statements: list):
    """
    Combines different sql conditions into one WHERE clause.

    Parameters:
    ----------
    condition_statements: list
        A list of sql conditions.

    Returns:
    --------
    str:
        A string in a SQL WHERE statement format.
    """
    condition_statements = [statement for statement in condition_statements if len(statement) > 0]
    if len(condition_statements) > 1:
        condition_statement = "WHERE " + " AND ".join(["(" + c + ")" for c in condition_statements])
    else:
        condition_statement = "WHERE " + condition_statements[0]
    return condition_statement
    
def keyword_query(keywords, matching_column, data_batch = "all", countries = "all", escape_expression = "@"):
    """
    Create a SQL query for a keyword search in a matching column and retrieve all uniq. 

    Parameters:
    ----------
    keywords : list
        A list of keywords to search in the matching variable.
    matching_variable: str
        A string specifiying the JPOD column to be searched for the keywords.
    data_batch: str, list[str]
        A string or list of strings indicating the data batches where the keyword query search should be performed. Default is "all".
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
   
    # subset the data to consider
    batch_condition = _generate_sql_batch_condition(data_batch = data_batch)
    country_condition = _generate_sql_country_condition(countries = countries)
    where_clause = _combine_sql_conditions(condition_statements = [batch_condition, country_condition])

    # define the keyword search
    like_statement = sql_like_statement(
        keywords = keywords, 
        matching_column = matching_column, 
        escape_expression = escape_expression
        )

    # define the sql query
    JPOD_QUERY = """
    SELECT uniq_id
    FROM (
        SELECT uniq_id, lower({0}) as {0}
        FROM job_postings
        {1}
        )
    WHERE ({2})
    """.format(matching_column, where_clause, like_statement)

    return JPOD_QUERY

class DuplicateCleaner():
    """
    Clean and identify duplicated job postings.
    """
    def __init__(self, con, data_batch):
        self.batch = data_batch
        self.con = con

    def duplicate_query(self, assign_to = "unique_posting_text", levels = ["job_description"]):
        """
        SQL-Query to identify duplicated job postings
        """
        level = ", ".join(levels)
        self.assign_to = assign_to
        
        jpod_query = """
        UPDATE job_postings 
        SET %s = 'no' 
        WHERE uniq_id IN (
            SELECT uniq_id
            FROM(
                SELECT uniq_id,
                ROW_NUMBER() OVER (PARTITION BY %s ORDER BY uniq_id) as rnr
                FROM(
                    SELECT jp.uniq_id, jp.job_description, pc.city, pc.inferred_country
                    FROM job_postings jp
                    LEFT JOIN position_characteristics pc on jp.uniq_id = pc.uniq_id
                    WHERE jp.data_batch = '%s' 
                    )
                )
            WHERE rnr > 1
        );
        """ % (assign_to, level, self.batch)

        return(jpod_query)

    def find_duplicates(self, query, commit: bool = True):
        """
        Run SQL-Query to identify and mark duplicated job postings in JPOD
        """
        self.con.execute(query)
        print("Duplicate cleaning successful for column '%s'" % self.assign_to)
        if commit:
            self.con.commit()
            print("JPOD changes commited.")

def load_jpod_nuts(conn):
    nuts_query = """
    SELECT name_en AS inferred_state, nuts_2, nuts_3
    FROM regio_grid
    WHERE nuts_level = 2 OR nuts_level = 3;
    """
    regio_nuts = pd.read_sql(con = conn, sql = nuts_query)

    oecd_query = """
    SELECT name_en AS inferred_state, oecd_tl2 AS nuts_2, oecd_tl3 AS nuts_3
    FROM regio_grid
    WHERE nuts_2 IS NULL AND nuts_3 IS NULL  AND (oecd_level = 2 OR oecd_level = 3);
    """
    regio_oecd = pd.read_sql(con = conn, sql = oecd_query)

    regions = pd.concat([regio_nuts, regio_oecd], axis= 0)
    return regions

def load_and_clean_keywords(keyword_file, multilingual = False):
    df = pd.read_csv(keyword_file)
    if not multilingual:
        keywords = list(set([w.replace(r"%", r"@%") for w in df["keyword_en"]]))
    else:
        for v in ["en", "de", "fr", "it"]:
            df["keyword_" + v] = [w.replace(r"%", r"@%") for w in df["keyword_" + v]]
            df["keyword_" + v] = [w.replace(r"_", r"@_") for w in df["keyword_" + v]]
            df["keyword_" + v] = [w.replace(r"'", r"''") for w in df["keyword_" + v]]
        keywords = []
        for v in ["en", "de", "fr", "it"]:
            keywords += list(df.loc[:, "keyword_" + v])
            keywords = list(set(keywords))
    return keywords

