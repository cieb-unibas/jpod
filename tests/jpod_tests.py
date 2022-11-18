import sqlite3
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd

def get_geo_dist(con, month = "full_sample", nuts_level = "2"):
    """
    Retrieve the spatial distribution of job postings for a specific temporal subset

    Parameters
    ----------
    con : sqlite3 object
        A sqlite3 connection to a jpod database.        
    month : str
        A string indicating the crawl time. The string must be specified as 'YYYY-MM'.
        For retrieving the full sample spatial distribution `month` can be specified as 'full_sample' (the default)
    nuts_level : str, int
        A string or int indicating the nuts level the distribution should be calculated for (default is '2')

    Returns
    -------
    pd.DataFrame :
        A pandas DataFrame consisting the number of postings retrieved across regions and their sample shares.
    """
    if isinstance(nuts_level, str) == False:
        nuts_level = str(nuts_level)
    if month == "full_sample":
        where_clause = ""
    else:
        where_clause = "WHERE pc.uniq_id IN (SELECT uniq_id FROM job_postings jp WHERE SUBSTR(crawl_timestamp, 1, 7) = '%s')" % month

    JPOD_QUERY = """
    SELECT COUNT(*) as total_postings, pc.nuts_{1}, rg.name_en region
    FROM (
        SELECT *
        FROM position_characteristics pc
        {0}
        ) pc
    LEFT JOIN (
        SELECT nuts_{1}, name_en
        FROM regio_grid
        WHERE nuts_level = {1}
        ) rg on pc.nuts_{1} = rg.nuts_{1}
    GROUP BY pc.nuts_{1}
    """.format(where_clause, nuts_level)
    dist = pd.read_sql_query(JPOD_QUERY, con = con)
    dist["postings_share"] = dist["total_postings"] / sum(dist["total_postings"])
    dist["sample"] = month
    return(dist)


def get_tech_share(con, month = "full_sample", tech = "bloom"):
    """
    Retrieve the share of postings with a connection to technological keywords for a specific temporal subset

    Parameters
    ----------
    con : sqlite3 object
        A sqlite3 connection to a jpod database.        
    month : str
        A string indicating the crawl time. The string must be specified as 'YYYY-MM'.
        For retrieving the full sample spatial distribution `month` can be specified as 'full_sample' (the default)
    tech : str
        A string indicating the technology that will be considered (one of 'bloom', 'ai'. Default is 'bloom')

    Returns
    -------
    pd.DataFrame :
        A pandas DataFrame consisting the number and shares of postings retrieved across temporal subsets.
    """
    if month == "full_sample":
        where_clause = ""
    else:
        where_clause = "WHERE SUBSTR(crawl_timestamp, 1, 7) = '%s'" % month
    
    assert tech in ["bloom", "ai"], "Invalid `tech` keyword '%s'. Pleaser specify one of 'bloom' or 'ai'." % tech
    if tech == "bloom":
        tech = "bloom_tech"
    else:
        tech = "acemoglu_ai"
    

    JPOD_QUERY = """
    SELECT COUNT(*) as total_postings
    FROM job_postings jp
    {0}
    """.format(where_clause)
    total_postings = con.execute(JPOD_QUERY).fetchone()[0]
    
    JPOD_QUERY = """
    SELECT COUNT(DISTINCT(uniq_id)) as tech_postings
    FROM {0}
    WHERE uniq_id IN (SELECT uniq_id FROM job_postings jp {1})
    """.format(tech, where_clause)
    res = pd.read_sql(JPOD_QUERY, con=con)
    
    res["sample"] = month
    res["tech"] = tech
    res["total_postings"] = total_postings
    res["share"] = res["tech_postings"] / res["total_postings"]
    return res

def get_spatial_tech(con, month = "full_sample", tech = "bloom"):
    """
    Retrieve the spatial distribution of job postings with a connection to technology fields
    for a specific temporal subset.

    Parameters
    ----------
    con : sqlite3 object
        A sqlite3 connection to a jpod database.        
    month : str
        A string indicating the crawl time. The string must be specified as 'YYYY-MM'.
        For retrieving the full sample spatial distribution `month` can be specified as 'full_sample' (the default)
    tech : str
        A string indicating the technology that will be considered (one of 'bloom', 'ai'. Default is 'bloom')

    Returns
    -------
    pd.DataFrame :
        A pandas DataFrame consisting the number and shares of postings retrieved across nuts_2 regions and temporal subsets.
    """
    if month == "full_sample":
        where_clause = ""
    else:
        where_clause = "WHERE SUBSTR(crawl_timestamp, 1, 7) = '%s'" % month
    
    assert tech in ["bloom", "ai"], "Invalid `tech` keyword '%s'. Pleaser specify one of 'bloom' or 'ai'." % tech
    if tech == "bloom":
        tech = "bloom_tech"
    else:
        tech = "acemoglu_ai"

    JPOD_QUERY = """
    SELECT COUNT(*) as n_postings, pc.nuts_2, rg.name_en region
    FROM (
        SELECT *
        FROM position_characteristics pc
        WHERE uniq_id IN (SELECT uniq_id FROM job_postings jp {0})
        AND uniq_id IN (SELECT uniq_id FROM {1})
        ) pc
    LEFT JOIN (
        SELECT nuts_2, name_en
        FROM regio_grid
        WHERE nuts_level = 2
        ) rg on pc.nuts_2 = rg.nuts_2
    GROUP BY pc.nuts_2
    """.format(where_clause, tech)
    res = pd.read_sql(JPOD_QUERY, con=con)

    res = res.dropna()
    res["sample"] = month
    res["tech"] = tech
    res["share"] = res["n_postings"] / sum(res["n_postings"])
    return res


def get_bloomfield_shares(con, month = "full_sample"):
    """
    Retrieve the distribution of postings with a connection to technological keywords from bloom across fields for a specific temporal subset

    Parameters
    ----------
    con : sqlite3 object
        A sqlite3 connection to a jpod database.        
    month : str
        A string indicating the crawl time. The string must be specified as 'YYYY-MM'.
        For retrieving the full sample spatial distribution `month` can be specified as 'full_sample' (the default)

    Returns
    -------
    pd.DataFrame :
        A pandas DataFrame consisting the number and shares of postings retrieved across different technology
        fields for a temporal subset.
    """
    if month == "full_sample":
        where_clause = ""
    else:
        where_clause = "WHERE SUBSTR(crawl_timestamp, 1, 7) = '%s'" % month
    
    JPOD_QUERY = """
    SELECT bt.bloom_field as field, COUNT(DISTINCT(uniq_id)) as tech_postings
    FROM bloom_tech bt
    WHERE uniq_id IN (SELECT uniq_id FROM job_postings jp {0})
    GROUP BY bloom_field
    """.format(where_clause)
    res = pd.read_sql(JPOD_QUERY, con=con)
    res["sample"] = month
    res["share"] = res["tech_postings"] / sum(res["tech_postings"])
    return res

def plot_groups(df, group, n_periods):
    """
    ...
    """
    groups = df.groupby([group])[group].count()
    groups = list(groups[groups == n_periods].index)
    df = df[df[group].isin(groups)]
    return df


def plot_dist(
    df, timewindow = "sample", group = "region",
    outcome = "postings_share", x_label = "Region", y_label = "Sample Share"
    ):
    """
    ...
    """
    n_periods = len(df[timewindow].drop_duplicates())
    n_groups = len(df[group].drop_duplicates())
    barWidth = 1 / (n_periods + 1)
    periods = [x for x in df[timewindow].drop_duplicates()]

    plt.figure(figsize=(21, 16))
    for i, p in enumerate(periods):
        if p == "full_sample":
            br = list(np.arange(n_groups))
        else:
            br = [x + (barWidth * i) for x in np.arange(n_groups)]
        y = df[df[timewindow] == p][outcome]
        plt.bar(x = br, height = y, width=barWidth, color = "C"+str(i), 
                edgecolor = "grey", label = p)
    plt.xlabel(x_label, fontweight ='bold', fontsize = 15)
    plt.ylabel(y_label, fontweight ='bold', fontsize = 15)
    plt.xticks([r + barWidth for r in range(n_groups)],
            list(df[group].drop_duplicates()), rotation = 45)
    plt.legend()

def get_employer_sample(con, n = 1000, min_postings = 1):
    """
    ....
    """
    JPOD_QUERY ="""
    SELECT company_name
    FROM position_characteristics
    GROUP BY company_name
    HAVING COUNT(*) >= %d
    ORDER BY RANDOM()
    LIMIT %d
    """ % (min_postings, n)
    sample_employers = con.execute(JPOD_QUERY).fetchall()
    sample_employers = [e[0] for e in sample_employers]
    if len(sample_employers) < n:
        print("Retrieved number of sample employers is", len(sample_employers), "and thus smaller than the specified number `n` =", n)
    return sample_employers

def get_employer_postings(con, employers):
    """"
    ...
    """
    JPOD_QUERY ="""
    SELECT jp.uniq_id, pc.company_name, jp.job_description
    FROM (
        SELECT uniq_id, job_description
        FROM job_postings
        WHERE uniq_id IN (
            SELECT uniq_id 
            FROM position_characteristics 
            WHERE company_name IN ({0})
            )
        ) jp
    LEFT JOIN (
        SELECT company_name, uniq_id
        FROM position_characteristics
        ) 
        pc on pc.uniq_id = jp.uniq_id
    """.format(str(employers)[1:-1])
    res = pd.read_sql(con = con, sql = JPOD_QUERY)
    return res

def get_3mtimewindow(month = "2021-05"):
    """
    ...
    """
    
    start_date = month + "-01"
    
    prev_month = datetime.date.fromisoformat(start_date) - datetime.timedelta(days=1)
    prev_month = prev_month.strftime("%Y-%m")
    
    next_month = datetime.date.fromisoformat(start_date) + datetime.timedelta(days=31)
    next_month = next_month.strftime("%Y-%m")
    
    time_window = [prev_month, month, next_month]
    return time_window

