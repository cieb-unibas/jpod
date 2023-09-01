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

#### configure -----------------------------------------------------------------
INCLUDE_SUBSIDIARIES <- FALSE
DB_VERSION <- "jpod.db"

#### Access JPOD ---------------------------------------------------------------
DB_DIR <- "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
DB_PATH <- paste0(DB_DIR, DB_VERSION)
JPOD_CONN <- dbConnect(RSQLite::SQLite(), DB_PATH)
if(exists("JPOD_CONN")){print("Connection to JPOD successfull")}

#### Define Output file namings
if(INCLUDE_SUBSIDIARIES == TRUE){
  out_file_extension <- "_inlc_subsidiaries"
}else{
  out_file_extension <- ""
}

#### Load data on firm patterns-------------------------------------------------
# parent companies and corresponding search tokens:
patterns_to_firms <- read.csv("./examples/pharma_ai/pharma_firm_list.csv")

# subsidiaries: 
if(INCLUDE_SUBSIDIARIES == TRUE){
  # load subsidiaries and search tokens:
  patterns_subsidiaries_firms <-  read.csv(
    "/scicore/home/weder/GROUP/Innovation/11_Classifications/top20_pharma.csv", sep = ";"
    ) %>%
    select(name, search_tokens) %>% 
    rename(firm_name = name, firm_name_pattern = search_tokens)
  # add to existing parent company names and search tokens
  patterns_to_firms <- rbind(patterns_to_firms, 
                             patterns_subsidiaries_firms[, c("firm_name_pattern", "firm_name")]
                             )
  # only keep every search token once
  patterns_to_firms <- patterns_to_firms %>% distinct(firm_name_pattern, .keep_all = TRUE)
}

#### Query Strings  ---------------------------------------------------------------
# Prepare a character string in SQL-Format to extract employers 
# whose name matches the patterns indicated in 'patterns_to_firms'
company_patterns <- paste0("'%", patterns_to_firms$firm_name_pattern, "%'")
company_patterns <- paste0("company_name LIKE ", company_patterns, collapse = " OR ")

exclude_tokens <- paste0("'%", c("bayern", "bayerisch", "rochester", "johanniter", "falcon farms", "broadcasting"), "%'")
exclude_tokens <- paste0("company_name NOT LIKE ", exclude_tokens, collapse = " AND ")

#### Query JPOD ---------------------------------------------------------------
# In order to get the total number of postings from these employers:
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

print(
  paste("Found", 
        length(unique(total_postings$company_name)), 
        "employer names matching the indicated patterns.")
  )

# in order to get the number of AI-related postings from these employers:
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

print(
  paste("Found", 
        length(unique(ai_postings$company_name)), 
        "employer names with ai postings matching the indicated patterns.")
  )

#### False positive tests  -----------------------------------------------------
# test whether there are some big false positive matches
# write to disk and check manually. if there are, drop them by inserting into
# `exclude_tokens` above and re-run this script.
test_firm_list <- total_postings %>% 
  group_by(company_name) %>%
  summarise(n_postings = sum(n_postings)) %>%
  filter(n_postings >= 50) %>%
  arrange(-n_postings)
if(nrow(test_firm_list) > 0){
  out_file = paste0("./examples/pharma_ai/pharma_test_firm_list", out_file_extension, ".csv")
  write.csv(test_firm_list, out_file, row.names = FALSE)
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

#### get and add regional names:-------------------------------------------
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

### Show the top-20 ranked companies:-------------------------------------------
print("Resulting top-20 companies:")
res %>% 
  group_by(company_name) %>% 
  summarise(
    n_ai_postings = sum(n_ai_postings),
    n_postings = sum(n_postings),
    ai_share = n_ai_postings / n_postings) %>%
  filter(ai_share > 0) %>%
  arrange(-ai_share) %>%
  head(20)

#### save the data and close the database --------------------------------------
out_file = paste0("./examples/pharma_ai/pharma_ai_shares", out_file_extension, ".csv")
write.csv(res, out_file, row.names = FALSE)

dbDisconnect(conn = JPOD_CONN)
