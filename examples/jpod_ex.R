#### Information for R and SQL Databases: https://db.rstudio.com/

#### Install/Load necessary R packages
pkgs <- c("RSQLite", "DBI", "dplyr")
package_setup <- function(packages){
  pkgs_available <- packages %in% installed.packages()
  for(p in seq(packages)){
    if(pkgs_available[p]){
      library(packages[p], character.only = TRUE)
    }else{
      install.packages(packages[p])
      library(packages[p], character.only = TRUE)
      }
  }
}
package_setup(packages = pkgs)

#### Access JPOD ---------------------------------------------------------
DB_DIR <- "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/jpod.db"
JPOD_CONN <- dbConnect(RSQLite::SQLite(), DB_DIR)
if(exists("JPOD_CONN")){print("Connection to JPOD successfull")}

#### Get JPOD Architecture -----------------------------------------------------
TABLES <- dbListTables(JPOD_CONN) # get tables
TABLES_VARS <- list() # get columns in each table
for(table in TABLES){
  TABLES_VARS[[table]] <- dbListFields(JPOD_CONN, table)
  }

#### Querying JPOD  with the "dplyr" package --------------------------------------
# Documentation: https://db.rstudio.com/r-packages/dplyr/
# 'dplyr' has a build in package 'dbplyr' that allows to query SQL databases
# using the familiar dplyr syntax. 
# The SQL functionality is somewhat limited and some R functions 
# are not recognized/transformed to SQL by dblyr. Example 3 presents such a case
# and provides a workaround option.

# EXAMPLE 1: number of job adds by job board
tbl(src = JPOD_CONN, "job_postings") %>%
  group_by(job_board) %>%
  summarize(n_postings = n()) %>%
  rename(plattform = job_board) %>%
  arrange(-n_postings)

# EXAMPLE 2: Biggest 10 cities with their total number of postings
tbl(src = JPOD_CONN, "position_characteristics") %>%
  filter(is.na(city) == FALSE) %>%
  group_by(city) %>%
  summarize(n_postings = n()) %>%
  arrange(-n_postings) %>%
  head(10)

# EXAMPLE 3: Job positions from a specific company (Migros) in a given city (Bern)
res <- tbl(src = JPOD_CONN, "position_characteristics") %>%
  filter(
    grepl(pattern = "bern", city) & 
    grepl(pattern = "migros", ignore.case = company_name)
    ) %>%
  select(company_name, city, inferred_job_title)
res # => produces an error since grepl() is not supported by 'dbplyr'
# check the SQL-transformed query:
show_query(res) # Pronlem is that grepl() is not transformed into a LIKE statement.
# Solution: use sql() directly instead
tbl(src = JPOD_CONN, "position_characteristics") %>%
  filter(
    sql("city LIKE 'bern' AND company_name LIKE '%migros%'")
    ) %>%
  select(company_name, city, inferred_job_title)

# EXAMPLE 4: Top 20 companies in terms of the number of postings using the term "machine learning"
tbl(src = JPOD_CONN, "job_postings") %>%
  filter(
    sql("job_description LIKE '%machine learning%'")
    ) %>%
  select(uniq_id) %>%
  left_join(
    tbl(src = JPOD_CONN, "position_characteristics"),
    by = "uniq_id") %>%
  group_by(company_name) %>%
  summarize(n_postings = n()) %>%
  arrange(-n_postings) %>%
  head(20)


#### Querying JPOD  with the "DBI" package --------------------------------------
# Documentation: https://www.rdocumentation.org/packages/DBI/versions/0.5-1
# 'DBI' has a backend that allows you to send SQL statements to databases. 
# However, these statements have to be written in SQL.

# helper function for retrieving results:
jpodRetrieve <- function(jpod_conn, sql_statement){
  res <- dbSendQuery(conn = jpod_conn, statement = sql_statement)
  df <- dbFetch(res)
  dbClearResult(res)
  return(df)
}

# EXAMPLE 1: number of job adds by job board
jpod_query = "
SELECT job_board as plattform, COUNT(*) as n_postings 
FROM job_postings 
GROUP BY job_board
ORDER BY n_postings DESC;
"
jpodRetrieve(jpod_conn = JPOD_CONN, sql_statement = jpod_query)

# EXAMPLE 2: Biggest 10 cities with their total number of postings
jpod_query = "
SELECT city, COUNT(city) n_postings
FROM position_characteristics 
GROUP BY city
HAVING city IS NOT 'nan'
ORDER BY n_postings DESC
LIMIT 10;
"
jpodRetrieve(jpod_conn = JPOD_CONN, sql_statement = jpod_query)

# EXAMPLE 3: Job positions from a specific company (Migros) in a given city (Bern)
jpod_query = "
SELECT company_name, city, inferred_job_title AS job_title
FROM position_characteristics
WHERE city LIKE 'bern' AND company_name LIKE '%migros%'
"
jpodRetrieve(jpod_conn = JPOD_CONN, sql_statement = jpod_query)

# EXAMPLE 4: Top 20 companies in terms of the number of postings using the term "machine learning"
KEYWORD = "machine learning"
jpod_query = paste0("
SELECT pc.company_name, COUNT(*) as n_postings
FROM (
    SELECT uniq_id, job_title, job_description
    FROM job_postings
    WHERE job_description LIKE '%", 
                    KEYWORD,
                    "%') jp
LEFT JOIN position_characteristics pc ON pc.uniq_id = jp.uniq_id
GROUP BY pc.company_name
ORDER BY n_postings DESC
LIMIT 20;")
paste("Employers asking about", KEYWORD)
jpodRetrieve(jpod_conn = JPOD_CONN, sql_statement = jpod_query)



