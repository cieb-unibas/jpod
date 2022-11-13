import sys
import pandas as pd
import sqlite3

# connect to the databse
DB_DIR = sys.argv[1]
DB_VERSION = sys.argv[2]
DB_DIR = "C:/Users/matth/Desktop/"
DB_VERSION = "jpod_test.db"
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)

def get_geo_dist(month = "full_sample", nuts_level = "2"):
    """
    Retrieve the spatial distribution of job postings by crawling month

    Parameters
    ----------
    month : str
        A string indicating the crawl time. The string must be specified as 'YYYY-MM'.
        For retrieving the full sample spatial distribution month can be specified as 'full_sample' (the default)
    nuts_level : str, int
        A string indicating the nuts level the distribution should be calculated for (default is '2')

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
    dist = pd.read_sql_query(JPOD_QUERY, con=JPOD_CONN)
    dist["postings_share"] = dist["total_postings"] / sum(dist["total_postings"])
    dist = dist.dropna()
    dist["sample"] = month
    return(dist)


def get_bloom_dist(month = "full_sample", spatial = False):
    """
    ...
    """
    return()


def get_ai_dist(month = "full_sample", spatial = False):
    """
    ...
    """
    return()


#### Distribution Comparison tests

# get nuts_2 distribution of full sample & snapshot sample from a particular month
geo_dist = get_geo_dist("full_sample", nuts_level="2")
for m in ["2021-05", "2021-09", "2021-01"]:
    geo_dist = pd.concat([geo_dist, get_geo_dist(month = m, nuts_level=2)])

# get nuts_3 distribution of full sample & snapshot sample
geo_dist = get_geo_dist("full_sample", nuts_level=3)
for m in ["2021-05", "2021-09", "2021-01"]:
    geo_dist = pd.concat([geo_dist, get_geo_dist(month = m, nuts_level=3)])

# get bloom_tech distribution of full sample & snapshot sample

# get ai distribution of full sample & snapshot sample

# get spatial distribution of bloom_tech in full sample & snapshot sample

# get spatial distribution of ai in full sample & snapshot sample
