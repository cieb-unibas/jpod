#### check if necessary packages are installed: --------------------------------
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

#### Access JPOD ---------------------------------------------------------------
DB_DIR <- "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
DB_VERSION <- "jpod_test.db"
DB_PATH <- paste0(DB_DIR, DB_VERSION)
JPOD_CONN <- dbConnect(RSQLite::SQLite(), DB_PATH)
if(exists("JPOD_CONN")){print("Connection to JPOD successfull")}

#### Load data on firm patterns-------------------------------------------------
patterns_to_firms <- read.csv("./examples/pharma_ai/pharma_firm_list.csv")

#### Prepare String in SQL-Format to extract employers whose name matches the patterns
company_patterns <- paste0("'%", patterns_to_firms$firm_name_pattern, "%'")
sql_query_string <- paste0("company_name LIKE ", company_patterns, collapse = " OR ")

#### Query JPOD to get the total number of postings from these employers:-------
total_postings <- tbl(src = JPOD_CONN, "institutions") %>%
  filter(sql(sql_query_string)) %>%
  select(company_name) %>%
  inner_join(
    tbl(src = JPOD_CONN, "position_characteristics"), 
    by = "company_name") %>%
  select(company_name) %>%
  filter(sql("company_name NOT LIKE '%bayern%' OR '%rochester%'")) %>%
  group_by(company_name) %>%
  summarize(n_postings = n()) %>%
  arrange(-n_postings) %>%
  as.data.frame()

#### Query JPOD to get the number of AI-related postings from these employers:--
ai_postings <- tbl(src = JPOD_CONN, "institutions") %>%
  filter(sql(sql_query_string)) %>%
  select(company_name) %>%
  inner_join(
    tbl(src = JPOD_CONN, "position_characteristics"), 
    by = "company_name") %>%
  select(company_name, uniq_id) %>%
  inner_join(tbl(src = JPOD_CONN, "acemoglu_ai"), 
             by = "uniq_id") %>%
  select(company_name) %>%
  filter(sql("company_name NOT LIKE '%bayern%' OR '%rochester%'")) %>%
  group_by(company_name) %>%
  summarize(n_ai_postings = n()) %>%
  arrange(-n_ai_postings) %>%
  as.data.frame()

#### Rename the company names retrieved from JPOD to their 'clean' name:--------
rename_companies <- function(company_names){
  for (i in 1:length(company_names)) {
    company_name <- company_names[i]
    
    for (j in 1:length(patterns_to_firms$firm_name_pattern)) {
      if (grepl(patterns_to_firms$firm_name_pattern[j], company_name)){
        company_name <- patterns_to_firms$firm_name[j]
        break
      }
    }
    company_names[i] <- company_name
    }
  return(company_names)
}

ai_postings$company_name <- rename_companies(company_names = ai_postings$company_name)
total_postings$company_name <- rename_companies(company_names = total_postings$company_name)

#### Aggregate postings for 'cleaned' companies and calculate ai-shares: -------
ai_postings <- ai_postings %>% group_by(company_name) %>% summarize(n_ai_postings = sum(n_ai_postings))
total_postings <- total_postings %>% group_by(company_name) %>% summarize(n_postings = sum(n_postings))
res <- ai_postings %>% 
  left_join(total_postings, by = "company_name") %>% 
  mutate(ai_share = n_ai_postings / n_postings) %>%
  arrange(-ai_share)

### Show the top-20 ranked companies and save the result:
print("Resulting top-20 companies:")
print(head(res, 20))
write.csv(res, "./examples/pharma_ai/pharma_ai_shares.csv", row.names = FALSE)
