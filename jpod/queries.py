import pandas as pd

from .datagen import _generate_sql_batch_condition, _generate_sql_unique_condition, _combine_sql_conditions

def get_region_names(con):
    region_names = pd.read_sql(
         sql= "SELECT name_en AS region, oecd_tl2 AS region_code FROM regio_grid WHERE oecd_level = 2",
         con=con)
    return region_names

def _set_nuts_level(regional_level: int = 2) -> str:
    if regional_level == 2:
        region_column = "nuts_2"
    elif regional_level == 3:
        region_column = "nuts_3"
    else:
        raise ValueError("Regional level must be either `2` or `3`.")
    return region_column

def _add_region_names_and_sort(con, df, sort_by):
    region_names = get_region_names(con = con)
    df_out = df.merge(region_names, how = "left", on="region_code").sort_values(sort_by, ascending=False).reset_index(drop=True)
    return df_out


def get_total_postings_by_region(con, regional_level=2, data_batch = "jobspickr_2023_01", unique_postings_only = True):
    """Get the number of job postings by regions"""
    region_column = _set_nuts_level(regional_level=regional_level)
    batch_condition = _generate_sql_batch_condition(data_batch=data_batch)
    uniq_condition = _generate_sql_unique_condition(unique_postings_only=unique_postings_only)
    where_clause = _combine_sql_conditions([batch_condition, uniq_condition])
 
    query = """
    SELECT pc.{0} AS region_code, COUNT(pc.uniq_id) AS n_postings
    FROM (
        SELECT uniq_id
        FROM job_postings jp
        {1}) jp
    LEFT JOIN position_characteristics pc ON pc.uniq_id = jp.uniq_id
    GROUP BY region_code
    """.format(region_column, where_clause)
    df = pd.read_sql(sql = query, con = con)
    df = _add_region_names_and_sort(con = con, df = df, sort_by="n_postings")
    return df

def get_ai_postings_by_region(con, regional_level=2, data_batch = "jobspickr_2023_01", unique_postings_only = True):
    """Get the number of ai-related job postings by regions"""
    region_column = _set_nuts_level(regional_level=regional_level)
    batch_condition = _generate_sql_batch_condition(data_batch=data_batch)
    uniq_condition = _generate_sql_unique_condition(unique_postings_only=unique_postings_only)
    where_clause = _combine_sql_conditions([batch_condition, uniq_condition])
 
    query = """
    SELECT pc.inferred_country AS country,
        pc.{0} AS region_code,
        COUNT(aa.uniq_id) AS n_ai_postings
    FROM acemoglu_ai aa
    LEFT JOIN (
        SELECT uniq_id, inferred_country, {0}
        FROM position_characteristics
        ) pc ON aa.uniq_id = pc.uniq_id
    INNER JOIN (
        SELECT uniq_id
        FROM job_postings
        {1}
        ) jp ON aa.uniq_id = jp.uniq_id
    GROUP BY country, region_code
    """.format(region_column, where_clause)
    df = pd.read_sql(sql = query, con = con)
    df = _add_region_names_and_sort(con = con, df = df, sort_by="n_ai_postings")
    return df

def get_bloom_postings_by_region(con, by_field = True, regional_level = 2, data_batch = "jobspickr_2023_01", unique_postings_only = True):
    """Get the number of bloom-technology-related job postings by regions"""
    region_column = _set_nuts_level(regional_level=regional_level)
    batch_condition = _generate_sql_batch_condition(data_batch=data_batch)
    uniq_condition = _generate_sql_unique_condition(unique_postings_only=unique_postings_only)
    where_clause = _combine_sql_conditions([batch_condition, uniq_condition])
    
    if by_field:
        query = """
        SELECT pc.inferred_country AS country,
            pc.{0} AS region_code,
            bt.bloom_field as technology_field,
            COUNT(bt.uniq_id) AS n_disruptive_tech_postings
        FROM bloom_tech bt
        LEFT JOIN (
            SELECT uniq_id, inferred_country, {0}
            FROM position_characteristics
            ) pc ON bt.uniq_id = pc.uniq_id
        INNER JOIN (
            SELECT uniq_id
            FROM job_postings
            {1}
            ) jp ON bt.uniq_id = jp.uniq_id
        GROUP BY country, region_code, technology_field
        """.format(region_column, where_clause)
    else:
        query = """
        SELECT pc.inferred_country AS country,
            pc.{0} AS region_code,
            COUNT(bt.uniq_id) AS n_disruptive_tech_postings
        FROM bloom_tech bt
        LEFT JOIN (
            SELECT uniq_id, inferred_country, {0}
            FROM position_characteristics
            ) pc ON bt.uniq_id = pc.uniq_id
        INNER JOIN (
            SELECT uniq_id
            FROM job_postings
            {1}
            ) jp ON bt.uniq_id = jp.uniq_id
        GROUP BY country, region_code
        """.format(region_column, where_clause)
    df = pd.read_sql(sql = query, con = con)
    df = _add_region_names_and_sort(con = con, df = df, sort_by="n_disruptive_tech_postings")
    return df

def get_remote_postings_by_region(con, regional_level = 2, data_batch = "jobspickr_2023_01", unique_postings_only = True):
    """Get the number of remote job postings by regions"""
    region_column = _set_nuts_level(regional_level=regional_level)
    batch_condition = _generate_sql_batch_condition(data_batch=data_batch)
    uniq_condition = _generate_sql_unique_condition(unique_postings_only=unique_postings_only)
    where_clause = _combine_sql_conditions([batch_condition, uniq_condition])
    
    query = """
    SELECT pc.inferred_country AS country,
        pc.{0} AS region_code,
        COUNT(pc.uniq_id) AS n_remote_postings
    FROM (
        SELECT uniq_id
        FROM job_postings
        {1}
    ) jp
    INNER JOIN (
        SELECT uniq_id, inferred_country, {0}
        FROM position_characteristics
        WHERE remote_position = 1
        ) pc ON pc.uniq_id = jp.uniq_id
    GROUP BY country, region_code
    """.format(region_column, where_clause)
    df = pd.read_sql(sql = query, con = con)
    df = _add_region_names_and_sort(con = con, df = df, sort_by="n_remote_postings")
    return df