import pandas as pd
import jpod

# connect to the database
JPOD_PATH = jpod.get_path(potential_paths=jpod.config.DB_DIRS)
JPOD_CONN = jpod.connect_to_jpod(db_location=JPOD_PATH, version="full")

# Get all ai jobs per region for the 2023 batch:
AI_QUERY = """
SELECT pc.inferred_country AS country,
    pc.nuts_2 AS region_code,
    COUNT(aa.uniq_id) AS n_ai_postings
FROM acemoglu_ai aa
LEFT JOIN (
    SELECT uniq_id, inferred_country, nuts_2
    FROM position_characteristics
    ) pc ON aa.uniq_id = pc.uniq_id
INNER JOIN (
    SELECT uniq_id
    FROM job_postings
    WHERE data_batch = 'jobspickr_2023_01' AND unique_posting_text = 'yes'
    ) jp ON aa.uniq_id = jp.uniq_id
GROUP BY country, region_code
"""
ai_jobs_per_region = pd.read_sql(con = JPOD_CONN, sql = AI_QUERY)

# Get overall number of jobs per region for the 2023 batch:
TOTAL_QUERY = """
SELECT pc.nuts_2 AS region_code, COUNT(pc.uniq_id) AS n_postings
FROM (
    SELECT uniq_id 
    FROM job_postings 
    WHERE data_batch = 'jobspickr_2023_01' AND unique_posting_text = 'yes'
    ) jp
LEFT JOIN position_characteristics pc ON jp.uniq_id = pc.uniq_id
GROUP BY region_code
"""
total_jobs_per_region = pd.read_sql(con = JPOD_CONN, sql = TOTAL_QUERY).drop_duplicates()

# connect and calculate ai intensity
ai_jobs_per_region = ai_jobs_per_region.merge(total_jobs_per_region, how="left", on="region_code")
ai_jobs_per_region["ai_share"] = ai_jobs_per_region["n_ai_postings"] / ai_jobs_per_region["n_postings"]

# get region names
regions = pd.read_sql(con=JPOD_CONN, 
                      sql= "SELECT name_en AS region, oecd_tl2 AS region_code FROM regio_grid WHERE oecd_level = 2")
ai_jobs_per_region = ai_jobs_per_region.merge(regions, how = "left", on="region_code")

# manually add some regions due to merging problems
#ai_jobs_per_region.loc[ai_jobs_per_region["region"].isna(),["region"]] = ["ile-de-france", "berlin", "hong kong", "east of england", "east of england", "south east of england"]

#present result:
ai_jobs_per_region = ai_jobs_per_region[["region", "region_code", "country", "n_ai_postings", "ai_share"]]
ai_jobs_per_region.to_csv("./data/ai_dist_region.csv", index=False)
print(ai_jobs_per_region.sort_values(by = "ai_share", ascending=False).reset_index(drop=True).head(10))