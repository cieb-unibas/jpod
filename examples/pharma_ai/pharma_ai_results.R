library("dplyr")

posting_counts <- read.csv("./examples/pharma_ai/pharma_ai_shares.csv")

print("Resulting top-20 comanies:")
posting_counts %>% 
  group_by(company_name) %>% 
  summarise(
    n_ai_postings = sum(n_ai_postings),
    n_postings = sum(n_postings),
    ai_share = n_ai_postings / n_postings) %>%
  filter(ai_share > 0 & n_postings > 100) %>%
  arrange(-ai_share)


print("Resulting top-20 comanies:")
posting_counts %>% 
  group_by(country) %>% 
  summarise(
    n_ai_postings = sum(n_ai_postings),
    n_postings = sum(n_postings),
    ai_share = n_ai_postings / n_postings) %>%
  arrange(-ai_share) %>%
  head(20)


