import pandas as pd
import jpod

# connect to the database
JPOD_PATH = jpod.get_path(potential_paths=jpod.config.DB_DIRS)
JPOD_CONN = jpod.connect_to_jpod(db_location=JPOD_PATH, version="full")

# Get all ai jobs per region for the 2023 batch:
AI_QUERY = """
SELECT pc.company_name AS employer, pc.inferred_country AS country, rg.name_en AS region, rg.oecd_tl2 AS region_code
FROM acemoglu_ai aa
INNER JOIN (
    SELECT uniq_id
    FROM job_postings
    WHERE data_batch = 'jobspickr_2023_01' AND unique_posting_text = 'yes'
    ) jp ON aa.uniq_id = jp.uniq_id
LEFT JOIN (
    SELECT uniq_id, inferred_country, company_name, nuts_2
    FROM position_characteristics
    ) pc ON aa.uniq_id = pc.uniq_id
LEFT JOIN(
    SELECT nuts_2, name_en, oecd_tl2
    FROM regio_grid
    WHERE oecd_level = 2
    ) rg ON rg.oecd_tl2 = pc.nuts_2;
"""
ai_jobs = pd.read_sql(con = JPOD_CONN, sql = AI_QUERY)

# Get overall number of jobs per region for the 2023 batch:
TOTAL_QUERY = """
SELECT pc.inferred_country AS country, rg.name_en AS region, pc.nuts_2 AS region_code, COUNT(pc.uniq_id) AS n_postings
FROM (
    SELECT uniq_id
    FROM job_postings
    WHERE data_batch = 'jobspickr_2023_01' AND unique_posting_text = 'yes'
    ) jp
LEFT JOIN (
    SELECT uniq_id, inferred_country, nuts_2
    FROM position_characteristics
    ) pc ON jp.uniq_id = pc.uniq_id
LEFT JOIN(
    SELECT name_en, oecd_tl2
    FROM regio_grid
    WHERE oecd_level = 2
    ) rg ON rg.oecd_tl2 = pc.nuts_2
GROUP BY pc.nuts_2
"""
total_jobs = pd.read_sql(con = JPOD_CONN, sql = TOTAL_QUERY)

# combine a calculate shares
df = ai_jobs["region"].value_counts().reset_index().rename(columns={"index": "region", "region": "ai_postings"})
df = df.merge(total_jobs, "left", on = ["region"])
df["ai_share"] = df["ai_postings"] / df["n_postings"]
print(df.sort_values(by = "ai_share", ascending=False).reset_index(drop=True))
