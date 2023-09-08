# Pharma-AI

This folder contains code to illustrate the use of JPOD.

## Challenge
Extract the share of AI-related job postings among all job postings by a set of (large) pharmaceutical companies.

## How to approach it?

See the script [`pharma_ai_data.R`](pharma_ai_data.R) for the overall process.

### **Step 1:** Configure options, connnect to JPOD and load data:
In this first step, set some parameters, which will be important subsequently:

- `DB_VERSION` can be either set to `"jpod.db"` or `"jpod_test.db"`. For experimentation on the RStudio interface, set it to the test option, and only change it to `"jpod.db"`, when sending the script to the scicore cluster. Given that information, a connection to JPOD will be established (and tested).
- `INCLUDE_SUBSIDIARIES` controls wether only the parent company of pharmaceutical conglomarates will be considered are all their subsidiaries will also be included (e.g., Genentech for Roche).
- `DISTINCT_POSTINGS_ONLY` controls whether or not postings with the exact same text (within a country) will be counted only once or multiple times (see the [technical documentation](../../docs/jpod_manual.md) for more information)

The corresponding code from the script looks as follows:

```r
#### configure -----------------------------------------------------------------
DB_VERSION <- "jpod.db"
INCLUDE_SUBSIDIARIES <- TRUE
DISTINCT_POSTINGS_ONLY <- FALSE

#### Access JPOD ---------------------------------------------------------------
DB_DIR <- "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
DB_PATH <- paste0(DB_DIR, DB_VERSION)
JPOD_CONN <- dbConnect(RSQLite::SQLite(), DB_PATH)
if(exists("JPOD_CONN")){print("Connection to JPOD successfull")}
```

Subsequently, keyword-data regarding pharmaceutical companies is loaded from the cluster as a dataframe `patterns_to_firms`. Either including or without subsidiaries. 
The corresponding code from the script looks as follows:

```r
#### Load data on firm patterns-------------------------------------------------
# parent companies and corresponding search tokens:
patterns_to_firms <- read.csv("./examples/pharma_ai/data/pharma_firm_list.csv")

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
```

### **Step 2:** Define SQL strings to extract postings from all indicated pharmaceutical companies.

After loading/defining the keyword patterns for comapnies, the next step is to actually query JPOD to retrieve job postings from companies that match to these keyword-patterns. To do this, we first define some character strings with SQL formatting that will be used to query JPOD. Note that `%` is a special character in SQL syntax which indicates that everything before or after the `%` is unimportant for matching.

The corresponding code from the script looks as follows:

```r
#### Query Strings  ---------------------------------------------------------------
# Prepare strings in SQL-Format for querying 

# 1) find employers whose name matches the patterns indicated in 'patterns_to_firms'
company_patterns_string <- paste0("'%", patterns_to_firms$firm_name_pattern, "%'")
company_patterns_string <- paste0("company_name LIKE ", company_patterns_string, collapse = " OR ")

# 2) exclude false positively matched employers from (1)
exclude_tokens_string <- c("bayern", "bayerisch", "rochester", "johanniter", "falcon farms", "broadcasting")
exclude_tokens_string <- paste0("'%", exclude_tokens_string, "%'")
exclude_tokens_string <- paste0("company_name NOT LIKE ", exclude_tokens_string, collapse = " AND ")

# 3) filter the data to consider by batch (= March 2023) and conisder postings only once.
data_filter_string <- "data_batch = 'jobspickr_2023_01'"
if(DISTINCT_POSTINGS_ONLY){
  data_filter_string <- paste0(data_filter_string, " AND unique_posting_text = 'yes'")
}
```

### **Step 3:** Extract the data from JPOD

Now it is time to actually retrieve data from JPOD using the dplyr syntax.

The corresponding code from the script looks as follows:

```r
#### Query JPOD ---------------------------------------------------------------
# In order to get the total number of postings from these employers:
# 1) Take all companies from the 'institutions' table that match the patterns indicated above
# 2) Take all of their job postings registered in the 'position_characteristics' table using inner_join()
# 3) Throw out companies that were wrongly matched by the keyowrd search in (1). This concerns e.g. 'bayern' instead of only 'bayer'
# 4) Only consider job postings from the 2023 march data batch.
# 5) Calculate the number of postings by each retrieved company in each region.
total_postings <- tbl(src = JPOD_CONN, "institutions") %>%
  filter(sql(company_patterns_string)) %>%
  select(company_name) %>%
  inner_join(
    tbl(src = JPOD_CONN, "position_characteristics"), 
    by = "company_name") %>%
  select(company_name, uniq_id, nuts_2) %>%
  filter(sql(exclude_tokens_string)) %>%
  inner_join(
    tbl(src = JPOD_CONN, "job_postings") %>% filter(sql(data_filter_string)), 
    by = "uniq_id") %>%
  group_by(company_name, nuts_2) %>%
  summarise(n_postings = n()) %>%
  arrange(-n_postings) %>%
  as.data.frame()

# in order to get the number of AI-related postings from these employers:
# 1) Take all companies from the 'institutions' table that match the patterns indicated above
# 2) Take all of their job postings registered in the 'position_characteristics' table using inner_join()
# 3) Throw out companies that were wrongly matched by the keyowrd search in (1). This concerns e.g. 'bayern' instead of only 'bayer'
# 4) Only consider job postings from the 2023 march data batch.
# 5) Only take those postings that are registered in the 'acemoglu_ai' table, i.e. those that are connected to AI keywords.
# 6) Calculate the number of AI-postings by each retrieved company.
ai_postings <- tbl(src = JPOD_CONN, "institutions") %>%
  filter(sql(company_patterns_string)) %>%
  select(company_name) %>%
  inner_join(
    tbl(src = JPOD_CONN, "position_characteristics"), 
    by = "company_name") %>%
  select(company_name, uniq_id, nuts_2) %>%
  filter(sql(exclude_tokens_string)) %>%
  inner_join(
    tbl(src = JPOD_CONN, "job_postings") %>% filter(sql(data_filter_string)), 
    by = "uniq_id") %>%
  inner_join(tbl(src = JPOD_CONN, "acemoglu_ai"), 
             by = "uniq_id") %>%
  select(company_name, nuts_2) %>%
  group_by(company_name, nuts_2) %>%
  summarize(n_ai_postings = n()) %>%
  arrange(-n_ai_postings) %>%
  as.data.frame()
```

The first code block extracts the total number of postings by region (`nuts_2`) for each indicated company that matches any pattern in the `company_patterns_string`: First it filters the jpod `institutions` table to all companies that match any of the specified patterns. It then retrieves all registered postings of these companies and their regional code by an `inner_join` with the jpod `position_characteristics` table. Next, by applying a second `inner_join` with the jpod `job_postings` table, only those job postings from the indicated `data_batch` (= March 2023) are considered. Finally, the number of retrieved job postings is calculated at the company-regional level (e.g., the number of postings by Roche in Northwestern Switzerland).

The second code block essentially performs the same step with one major difference. After extracting all postings from March 2023 of companies that match any of the specified patterns, an additional `inner_join` to the jpod `acemoglu_ai` table is applied. Thus, only those postings that are related to AI keywords are retained (i.e., those also registered in the `acemoglu_ai` table).

### **Step 4:** Test and process the retrieved data

Since the job postings retrieval is based on keyword matching, there is the possibility of some false postive matches. An example that has been taken care of already is that of `roche`, which e.g., also matches `rochester`. Checking this does not involve any interaction with the database and is now done purely in R: The following testing code, which lists all matched companies with at least 50 postings, is added to control for false positive matches:

```r
test_firm_list <- total_postings %>% 
  group_by(company_name) %>%
  summarise(n_postings = sum(n_postings)) %>%
  filter(n_postings >= 50) %>%
  arrange(-n_postings)
if(nrow(test_firm_list) > 0){
  out_file = paste0("./examples/pharma_ai/data/pharma_test_firm_list", out_file_extension, ".csv")
  write.csv(test_firm_list, out_file, row.names = FALSE)
  }
```

The resulting `.csv` can be manually checked, and if false postive matches occured, you can add them to the `exclude_tokens_string` specified above.

Furthermore, the extracted postings must now be aggregated at the parent-company-level. This does again not involve any interaction with JPOD but simply relabels company names according to the `patterns_to_firms` data.frame, aggregates the number of postings at the parent-company-region level and calculates AI-shares for every parent company in every region:

```r
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
```

### **Step 5:** Add regional names and save result

Since regional information was retrieved from the `position_characteristics` table, there are only regional codes (at the oecd_tl2/nuts_2 level) but not regional codes specified. To add regional names, we can retrieve them from the jpod `regio_grid` table and add them to our data by standard dplyr syntax:

```r
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
```

Thats it. The data now has all information to perform several analyses. To do this, we save the retrieved data to a `.csv` on the cluster as follows:
```r
write.csv(res, out_file, row.names = FALSE)
```

### **Step 6:** Use the cluster to access the fully specifed database:

So far, we only queried the `jpod_test.db` database, since this can be handeled the scicore login node or your local machine. After testing that everything works, we can send the entire script to the scicore cluster, which will query the fully specified database `jpod.db`. To do this, first change the configuration parameter `DB_VERSION` to `"jpod.db"` at the top of the script.

```r
DB_VERSION <- "jpod.db"
```

Save the script and then use the shell script `pharma_ai_scicore.sh` to send your `R` code to the cluster. The code in `pharma_ai_scicore.sh` looks as follows:

```sh
#!/bin/bash
#SBATCH --job-name=pharma_ai_share
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G

#SBATCH --time=01:30:00
#SBATCH --qos=6hours

#SBATCH --output=examples/pharma_ai/pharma_ai_share
#SBATCH --error=examples/pharma_ai/pharma_ai_share_errors
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

## configure
## !!!  make sure that your working directory when sending this script to the cluster
## corresponds to the jpod repository. Use `cd PATH_TO_JPOD_REPOSITORY` to do that in the console 
## or uncomment and adapte the line below !!!
## cd PATH_TO_YOUR_JPOD_REPOSITORY_ON_SCICORE

## run the script
ml load R
Rscript ./examples/pharma_ai/pharma_ai_data.R
```

See the scicore documentation regarding the different instructions in this scirpt. What it essentially does is that it sends the code in [`pharma_ai_data.R`](pharma_ai_data.R) to the cluster for computation. 