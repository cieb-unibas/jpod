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

#### Set working directory and load data on firm patterns-------------------------------------------------
patterns_to_firms <- read.csv("./examples/pharma_ai/pharma_firm_list.csv")

#### Prepare a character string in SQL-Format to extract employers whose name matches the patterns indicated in 'patterns_to_firms'
company_patterns <- paste0("'%", patterns_to_firms$firm_name_pattern, "%'")
company_patterns <- paste0("company_name LIKE ", company_patterns, collapse = " OR ")

exclude_tokens <- paste0("'%", c("bayern", "bayerisch", "rochester"), "%'")
exclude_tokens <- paste0("company_name NOT LIKE ", exclude_tokens, collapse = " AND ")

#### Query JPOD to get the total number of postings from these employers:-------
# 1) Take all companies from the 'institutions' table that match the patterns indicated above
# 2) Take all of their job postings registered in the 'position_characteristics' table using inner_join()
# 3) Throw out companies that were wrongly matched by the keyowrd search in (1). This concerns e.g. 'bayern' instead of only 'bayer'
# 4) Only consider job postings from the 2023 march data batch.
# 5) Calculate the number of postings by each retrieved company in each region.
total_postings <- tbl(src = JPOD_CONN, "institutions") %>%
  filter(sql(company_patterns)) %>%
  select(company_name) %>%
  inner_join(
    tbl(src = JPOD_CONN, "position_characteristics"), 
    by = "company_name") %>%
  select(company_name, uniq_id, nuts_2) %>%
  filter(sql(exclude_tokens)) %>%
  inner_join(
    tbl(src = JPOD_CONN, "job_postings") %>% filter(data_batch == "jobspickr_2023_01"), 
    by = "uniq_id") %>%
  group_by(company_name, nuts_2) %>%
  summarise(n_postings = n()) %>%
  arrange(-n_postings) %>%
  as.data.frame()
print(paste("Found", length(unique(total_postings$company_name)), "employer names matching the indicated patterns."))

#### Query JPOD to get the number of AI-related postings from these employers:--
# 1) Take all companies from the 'institutions' table that match the patterns indicated above
# 2) Take all of their job postings registered in the 'position_characteristics' table using inner_join()
# 3) Throw out companies that were wrongly matched by the keyowrd search in (1). This concerns e.g. 'bayern' instead of only 'bayer'
# 4) Only consider job postings from the 2023 march data batch.
# 5) Only take those postings that are registered in the 'acemoglu_ai' table, i.e. those that are connected to AI keywords.
# 6) Calculate the number of AI-postings by each retrieved company.
ai_postings <- tbl(src = JPOD_CONN, "institutions") %>%
  filter(sql(company_patterns)) %>%
  select(company_name) %>%
  inner_join(
    tbl(src = JPOD_CONN, "position_characteristics"), 
    by = "company_name") %>%
  select(company_name, uniq_id, nuts_2) %>%
  filter(sql(exclude_tokens)) %>%
  inner_join(
    tbl(src = JPOD_CONN, "job_postings") %>% filter(data_batch == "jobspickr_2023_01"), 
    by = "uniq_id") %>%
  inner_join(tbl(src = JPOD_CONN, "acemoglu_ai"), 
             by = "uniq_id") %>%
  select(company_name, nuts_2) %>%
  group_by(company_name, nuts_2) %>%
  summarize(n_ai_postings = n()) %>%
  arrange(-n_ai_postings) %>%
  as.data.frame()
print(paste("Found", length(unique(ai_postings$company_name)), "employer names with ai postings matching the indicated patterns."))

#### test whether there are some big false positives:
test_firm_list <- total_postings %>% 
  group_by(company_name) %>%
  summarise(n_postings = sum(n_postings)) %>%
  filter(n_postings >= 100)
if(nrow(test_firm_list) > 0){
  write.csv(test_firm_list, "./examples/pharma_ai/pharma_test_firm_list.csv", row.names = FALSE)
  }

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

#### Aggregate postings for 'cleaned' companies and region and calculate ai-shares: -------
ai_postings <- ai_postings %>% 
  group_by(company_name, nuts_2) %>% 
  summarize(n_ai_postings = sum(n_ai_postings))
total_postings <- total_postings %>% 
  group_by(company_name, nuts_2) %>% 
  summarize(n_postings = sum(n_postings))
res <- total_postings %>%
  full_join(ai_postings, by = c("company_name", "nuts_2")) %>% 
  mutate(
    ai_share = n_ai_postings / n_postings,
    n_ai_postings = ifelse(is.na(n_ai_postings), 0, n_ai_postings)
  )

#### get and add regional names:
regions <- tbl(src = JPOD_CONN, "regio_grid") %>%
  filter(oecd_level == 2) %>%
  select(name_en, nuts_2, oecd_tl2, iso_2) %>%
  mutate(
    nuts_2 = ifelse(is.na(nuts_2), oecd_tl2, nuts_2),
    oecd_tl2 = ifelse(is.na(nuts_2), nuts_2, oecd_tl2),
  ) %>% rename(country = iso_2) %>% as.data.frame()
res <- res %>% 
  mutate(nuts_2 = ifelse(startsWith(nuts_2, "UK"), substr(nuts_2, 1,3), nuts_2)) %>%
  left_join(regions %>% select(-oecd_tl2), by = "nuts_2")

### Show the top-20 ranked companies and save the result:
print("Resulting top-20 comany-regions:")
res %>% 
  group_by(company_name) %>% 
  summarise(
    n_ai_postings = sum(n_ai_postings),
    n_postings = sum(n_postings),
    ai_share = n_ai_postings / n_postings) %>%
  filter(ai_share > 0) %>%
  head(20)
write.csv(res, "./examples/pharma_ai/pharma_ai_shares.csv", row.names = FALSE)
