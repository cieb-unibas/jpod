import sqlite3
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import datetime

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

def jpod_month_condition(months = "full_sample"):
    """
    Create a sql WHERE statement to subset JPOD to postings from a given period.
    
    Parameters
    ----------
    months : list
        A list of strings indicating the months to consider. The strings must be specified as 'YYYY-MM'.
    month : str
        A string or list of strings indicating the months to consider. The strings must be specified as 'YYYY-MM'.
        For specifiying no condition `month` can be specified as 'full_sample' which then returns an empty string (the default)

    Returns
    -------
    str :
        A sql WHERE condition for subsetting postings in JPOD to those of the specified months.
    """
    if months != "full_sample":
        assert isinstance(months, list), "Months to consider must be specified as a list of strings"
        months = "WHERE uniq_id IN (SELECT uniq_id FROM job_postings WHERE SUBSTR(crawl_timestamp, 1, 7) IN ({0}))".format(str(months)[1:-1])
    else:
        months = ""
    return months

def get_employer_sample(con, n = 1000, min_postings = 1, months = "full_sample"):
    """
    Retrieve a random sample of employers with a minimum number of postings within the full sample or a temporal subsample. 
    
    Parameters
    ----------
    con : sqlite3 object
        A sqlite3 connection to the JPOD database.
    n :
        A integer indicating the number of employers to retrieve.        
    min_postings : int
        An integer indicating the minimum number of postings employers should have within the specified timeframe 
    months = list
        A list of strings indicating the months to consider. The strings must be specified as 'YYYY-MM'.
        For specifiying no condition, `month` can be set to 'full_sample'.

    Returns
    -------
    list :
        A list of `n` employers with at least `min_postings` job adds retrieved from the selected `months`. If the
        number of retireved employers is smaller than the specified number `n`, a message is printed.
    """
    month_condition = jpod_month_condition(months = months)

    JPOD_QUERY ="""
    SELECT company_name
    FROM (
        SELECT *
        FROM position_characteristics
        %s
        )
    GROUP BY company_name
    HAVING COUNT(*) >= %d
    ORDER BY RANDOM()
    LIMIT %d
    """ % (month_condition, min_postings, n)
    sample_employers = con.execute(JPOD_QUERY).fetchall()
    sample_employers = [e[0] for e in sample_employers]
    if len(sample_employers) < n:
        print("Retrieved number of sample employers is", len(sample_employers), "and thus smaller than the specified number `n` =", n)
    return sample_employers


def get_employer_postings(con, employers, months = "full_sample"):
    """"
    Retrieve all the job postings for a selection of employers within a given time frame. 
    
    Parameters
    ----------
    con : sqlite3 object
        A sqlite3 connection to the JPOD database.
    employers : list
        A list of strings indicating the the employers. This names will be matched to the `company_name` column in JPOD. 
    months = list
        A list of strings indicating the months to consider. The strings must be specified as 'YYYY-MM'.
        For specifiying no condition, `month` can be set to 'full_sample'.

    Returns
    -------
    pd.DataFrame :
        A pandas DataFrame consisting consisting of the full texts of all employers' job postings in this time window.
    """
    months = jpod_month_condition(months = months)
    if len(months) > 0:
        months = months + " AND"
    else:
        months = "WHERE"

    JPOD_QUERY ="""
    SELECT jp.uniq_id, pc.company_name, jp.job_description
    FROM (
        SELECT uniq_id, job_description
        FROM job_postings
        {0} uniq_id IN (
            SELECT uniq_id 
            FROM position_characteristics 
            WHERE company_name IN ({1})
            )
        ) jp
    LEFT JOIN (
        SELECT company_name, uniq_id
        FROM position_characteristics
        ) 
        pc on pc.uniq_id = jp.uniq_id
    """.format(months, str(employers)[1:-1])
    res = pd.read_sql(con = con, sql = JPOD_QUERY)
    return res

def get_timewindow(month, month_window = 1):
    """
    Retrieve a selection of months that define a timewindow for queriying JPOD.

    Parameters
    ----------
    month : str
        A string indicating the base month. The string must be specified as 'YYYY-MM'.
    month_window : int
        A integer indicating how many previous and subsequent months from the baseline `month` 
        should be considered for the time-window (default is 1). 

    Returns
    -------
    list :
        A list consisting of all months in iso-format 'YYYY-MM' that are part of the specified time-window.

    """
    assert isinstance(month_window, int) and month_window >= 0, "`month_window` must be of type `int` and >= 0"

    start_date = month + "-01"
    n_months = range(month_window)

    prev_months = [datetime.date.fromisoformat(start_date) - datetime.timedelta(days = 1 + (31 * m)) for m in n_months]
    prev_months = [m.strftime("%Y-%m") for m in prev_months]
    subsequent_months = [datetime.date.fromisoformat(start_date) + datetime.timedelta(days = 31 * (m + 1)) for m in n_months]
    subsequent_months = [m.strftime("%Y-%m") for m in subsequent_months]

    time_window = prev_months + [month] + subsequent_months

    return time_window


def random_token_sequences_from_text(text, sequence_length = 10, n_sequences = 5):
    """
    Retrieve randomly chosen, non-overlapping sequences of tokens from a text.
    
    Parameters
    ----------
    text : str
        A text from which token sequences will be extracted
    sequence_length : int
        An integer indicating the number of tokens per sequence.        
    n_sequences : int
        An integer indicating the number of sequences to extract. 

    Returns
    -------
    list :
        A list of length `n_sequences`. Each element is a string representing a 
        token sequences of length sequence_length.
    """
    assert isinstance(sequence_length, int), "`sequence_length` must be of type 'int'"
    assert isinstance(n_sequences, int), "`n_sequences` must be of type 'int'"

    tokenized_text = text.split(" ")
    
    # if posting is too short to extract sequences, cut it in half and create two sequences:
    if len(tokenized_text) < (n_sequences * sequence_length * 2):
        cut_point = round(np.median(range(len(tokenized_text))))
        token_sequences = [text[:cut_point], text[cut_point:]]

    else:
        # evaluate if sequences will be non-overlapping 
        # That is, none of the randonly chosen starting points 
        # can lie within the defined sequence span of another  
        eval_distance = False
        while eval_distance == False:
            # randomly choose sequence starting points
            token_sequence_start = np.random.randint(0, len(tokenized_text) - sequence_length, n_sequences)
            # calculate distances between starting points
            eval_distance = []
            starting_distances = [list(abs(token_sequence_start - token_sequence_start[i])) for i in range(len(token_sequence_start))]
            for x in range(len(starting_distances)):
                starting_distances[x].remove(0)
                res = sum([e > sequence_length for e in starting_distances[x]]) >= (n_sequences - 1)
                eval_distance.append(res)
            eval_distance = all(eval_distance)
        
        token_sequences = []
        for s in token_sequence_start:
            token_sequences.append(tokenized_text[s: (s + sequence_length)])
        token_sequences = [" ".join(s) for s in token_sequences]

    return(token_sequences)