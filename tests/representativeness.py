#### Representatives ####
# BFS employment data taken for 2Q22 from: 
# https://www.pxweb.bfs.admin.ch/pxweb/de/px-x-0602000000_102/-/px-x-0602000000_102.px/
# bfs <- read.csv(
#   file = "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/augmentation_data/ch_total_employed_people_02_2022.csv",
#   sep = ";")
# df <- read.csv("data/plot1_df.csv")
# df <- df %>% 
#   na.omit() %>%
#   mutate(ch_total_share = total_postings / df[df$nuts_2 == "CH0", ]$total_postings[2]) %>%
#   select(nuts_2, Grossregion, ch_total_share) %>%
#   merge(bfs, by = "nuts_2") %>%
#   select(nuts_2, Grossregion, employed_share, ch_total_share) %>%
#   mutate(diff_share = ch_total_share / employed_share) %>%
#   arrange(diff_share)
# df

# nuts_2              Grossregion employed_share ch_total_share diff_share
# 1   CH07                   ticino     0.04544429     0.01018492  0.2241187
# 2   CH01         région lemanique     0.19204901     0.10411934  0.5421498
# 3   CH05      eastern switzerland     0.12749293     0.11130664  0.8730417
# 4   CH02        espace mittelland     0.20242806     0.17744788  0.8765972
# 5   CH03 northwestern switzerland     0.13296988     0.13513996  1.0163200
# 6   CH06      central switzerland     0.09873514     0.11616087  1.1764896
# 7   CH04                   zurich     0.20088068     0.28011824  1.3944508