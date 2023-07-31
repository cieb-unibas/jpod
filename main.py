import pandas as pd
import jpod

# connect to the database
JPOD_PATH = jpod.get_path(potential_paths=jpod.config.DB_DIRS)
JPOD_CONN = jpod.connect_to_jpod(db_location=JPOD_PATH, version="test")

pd.DataFrame(JPOD_CONN.execute("SELECT name_en, nuts_2, nuts_3, oecd_tl2, oecd_tl3 FROM regio_grid WHERE iso_2 = 'CN'").fetchall())
jpod.get_table_vars(conn=JPOD_CONN, table = "regio_grid")

JPOD_CONN.execute("SELECT DISTINCT inferred_state, nuts_2 FROM position_characteristics WHERE inferred_country = 'austria'").fetchall()
query="""
SELECT inferred_country, inferred_state, pc.nuts_2--, COUNT(aa.uniq_id)
FROM acemoglu_ai aa
INNER JOIN (
    SELECT * 
    FROM
    position_characteristics 
    WHERE inferred_country = 'germany') pc ON aa.uniq_id = pc.uniq_id
LEFT JOIN (SELECT uniq_id
    FROM job_postings
    WHERE data_batch = 'jobspickr_2023_01' AND unique_posting_text = 'yes'
    ) jp ON aa.uniq_id = jp.uniq_id
LEFT JOIN(
    SELECT nuts_2, name_en, oecd_tl2
    FROM regio_grid
    WHERE oecd_level = 2
    ) rg ON rg.oecd_tl2 = pc.nuts_2
--GROUP BY inferred_country, inferred_state, nuts_2
"""
pd.DataFrame(JPOD_CONN.execute(query).fetchall())


# Get all ai jobs per region for the 2023 batch:
AI_QUERY = """
SELECT country, region, region_code, n_postings
FROM (
    SELECT pc.inferred_country AS country,
        pc.nuts_2 AS region_code,
        COUNT(aa.uniq_id) AS n_postings
    FROM acemoglu_ai aa
    INNER JOIN (
        SELECT uniq_id, inferred_country, company_name, inferred_state, nuts_2
        FROM position_characteristics
        ) pc ON aa.uniq_id = pc.uniq_id
    LEFT JOIN (
        SELECT uniq_id
        FROM job_postings
        WHERE data_batch = 'jobspickr_2023_01' AND unique_posting_text = 'yes'
        ) jp ON aa.uniq_id = jp.uniq_id
    GROUP BY country, region_code
)
LEFT JOIN (
    SELECT name_en as region, oecd_tl2
    FROM regio_grid
    ) rg ON rg.oecd_tl2 = region_code
"""
ai_jobs = pd.read_sql(con = JPOD_CONN, sql = AI_QUERY)
ai_jobs

JPOD_CONN.execute("UPDATE position_characteristics SET nuts_2 = 'AT13' WHERE inferred_state = 'wien';")

# TO DO: 
# => change nuts_2 value of Berlin to 'DE3' AND ile-de-france to FR1 and wien to 'AT13'
# => change all the pc.nuts_2 to oecd_tl2 und oecd_tl3
# => make this as a left-join pc & rg where oecd_level = 2 ON PC.inferred_state = rg.region_name
# => set hong kong to 'CNHK'

# Get overall number of jobs per region for the 2023 batch:
# ...


# combine a calculate shares
df = ai_jobs["region"].value_counts().reset_index().rename(columns={"index": "region", "region": "ai_postings"})
df = df.merge(total_jobs, "left", on = ["region"])
df["ai_share"] = df["ai_postings"] / df["n_postings"]
print(df.sort_values(by = "ai_share", ascending=False).reset_index(drop=True))
