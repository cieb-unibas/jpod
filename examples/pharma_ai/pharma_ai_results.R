library("dplyr")

#### helper functions: --------------------------------------------------------
load_data <- function(include_subsidaries, distinct_only){
  # define the file
  if(include_subsidaries == TRUE){
    out_file_extension <- "_incl_subsidiaries"
  }
  if(distinct_only){
    if(exists("out_file_extension")){
      out_file_extension <- paste0(out_file_extension, "_distinct")
    }else{
      out_file_extension <- "_distinct"
    }
  }
  if(exists("out_file_extension")){
    file <- paste0("./examples/pharma_ai/data/pharma_ai_shares", out_file_extension, ".csv")
  }else{
    file <- "./examples/pharma_ai/data/pharma_ai_shares.csv"
  }

  # load the data
  posting_counts <- read.csv(file)
  return(posting_counts)  
}

company_ai_shares <- function(df, min_postings = 100){
  res <- df %>% 
    group_by(company_name) %>% 
    summarise(
      n_ai_postings = sum(n_ai_postings),
      n_postings = sum(n_postings),
      ai_share = n_ai_postings / n_postings) %>%
    filter(ai_share > 0 & n_postings > min_postings) %>%
    arrange(-ai_share)
  return(res)
}

log_string <- function(include_subsidaries, distinct_only){
  subsidaries_string <- ifelse(include_subsidaries, "including subsidiaries", "without subsidaries")
  distinct_string <- ifelse(distinct_only, "for distinct postings.", "for all registered postings.")
  log_string <- paste("Returning data", subsidaries_string, distinct_string)
  return(log_string)
}

get_top_companies <- function(include_subsidaries, distinct_only, min_postings = 100){
  # load data
  postings_counts <- load_data(include_subsidaries = include_subsidaries, distinct_only = distinct_only)
  # calculate company shares
  res <- company_ai_shares(df = postings_counts, min_postings = min_postings)
  # log:
  print(log_string(include_subsidaries, distinct_only))
  return(res)
}



# results: --------------------------------------------------------------------
top20 <- get_top_companies(
  include_subsidaries = FALSE, 
  distinct_only = TRUE, 
  min_postings = 50
  )
top20 %>% head(20)


