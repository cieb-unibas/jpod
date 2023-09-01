library("dplyr")

# configure
INCLUDE_SUBSIDIARIES <- FALSE

# load the data
if(INCLUDE_SUBSIDIARIES == TRUE){
  out_file_extension <- "_inlc_subsidiaries"
}else{
  out_file_extension <- ""
}
posting_counts <- read.csv("./examples/pharma_ai/pharma_ai_shares_inlc_subsidiaries.csv")

# print results:
print("Resulting top-20 comanies:")
res <- posting_counts %>% 
  group_by(company_name) %>% 
  summarise(
    n_ai_postings = sum(n_ai_postings),
    n_postings = sum(n_postings),
    ai_share = n_ai_postings / n_postings) %>%
  filter(ai_share > 0 & n_postings > 100) %>%
  arrange(-ai_share)
print(head(res, 20))


print("Resulting top-20 regions:")
res <- posting_counts %>% 
  group_by(name_en) %>% 
  summarise(
    n_ai_postings = sum(n_ai_postings),
    n_postings = sum(n_postings),
    ai_share = n_ai_postings / n_postings) %>%
  arrange(-ai_share) %>%
  rename(region = name_en)
print(head(res, 20))


