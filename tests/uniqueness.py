#### along the following lines:
# # firm <- 'meag munich ergo assetmanagement gmbh'
# # firm <- 'login berufsbildung ag'
# # firm <- 'bouygues energies & services'
# firm <- 'galliker transport ag'
# # firm <- 'facebook'
# JPOD_QUERY <- paste0("
# SELECT jp.job_description
# --SELECT COUNT(*) AS total_postings, COUNT(DISTINCT(jp.job_description)) AS n_unique
# FROM job_postings jp
# WHERE jp.uniq_id IN (
#   SELECT uniq_id 
#   FROM position_characteristics 
#   WHERE company_name == '", firm, "'
#   ) 
# LIMIT 2
# ")
# jpodRetrieve(jpod_conn = JPOD_CONN, sql_statement = JPOD_QUERY)

