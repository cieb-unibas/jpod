import sqlite3
import sys
import pandas as pd
from matplotlib import pyplot as plt
import jpod_tests

# connect to the databse --------------------------------
DB_DIR = sys.argv[1]
DB_VERSION = sys.argv[2]
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)

# A) get nuts_2 distribution of full sample & snapshot sample from a particular month --------------------------------
geo_dist = jpod_tests.get_geo_dist(con = JPOD_CONN, month="full_sample", nuts_level="2")
months = ["2021-05", "2021-09", "2021-01"]
for m in months:
    geo_dist = pd.concat([geo_dist, jpod_tests.get_geo_dist(con =JPOD_CONN, month = m, nuts_level=2)])
print("Number of postings per time window:")
print(geo_dist.groupby(["sample"])["total_postings"].sum())
#Number of postings per time window:
#sample
#2021-01         153646
#2021-05         175460
#2021-09         165916
#full_sample    3211219
plot_df = geo_dist.dropna()
jpod_tests.plot_dist(df = plot_df)
plt.legend()
plt.savefig("tests/img/nuts_2_dist.png")

# B) get nuts_3 distribution of full sample & snapshot sample --------------------------------
geo_dist = jpod_tests.get_geo_dist(con = JPOD_CONN, month = "full_sample", nuts_level=3)
months = ["2021-05", "2021-09", "2021-01"]
for m in months:
    geo_dist = pd.concat([geo_dist, jpod_tests.get_geo_dist(con = JPOD_CONN,month = m, nuts_level=3)])
plot_df = geo_dist.dropna()
plot_df = plot_df[plot_df["region"] != "glarus"]
jpod_tests.plot_dist(df = plot_df)
plt.legend()
plt.savefig("tests/img/nuts_3_dist.png")

# C) get share of postings with connection to overall bloom_tech in full sample & snapshot sample --------------------------------
tech = "bloom"
months = ["2021-05", "2021-09", "2021-01"]
tech_shares = jpod_tests.get_tech_share(con = JPOD_CONN, month = "full_sample", tech = tech)
for m in months:
    tech_shares = pd.concat([tech_shares, jpod_tests.get_tech_share(con = JPOD_CONN, month = m, tech = tech)])
print("Distribution of job postings with connection to Bloom (2021) disruptive technologies:")
print(tech_shares)
#Distribution of job postings with connection to Bloom (2021) disruptive technologies:
#   tech_postings       sample        tech  total_postings     share
#0         201543  full_sample  bloom_tech         3211219  0.062762
#0          10457      2021-05  bloom_tech          175460  0.059598
#0           8070      2021-09  bloom_tech          165916  0.048639
#0           8578      2021-01  bloom_tech          153646  0.055830

# D) get bloom field distribution of full sample & snapshot sample --------------------------------
months = ["2021-05", "2021-09", "2021-01"]
field_shares = jpod_tests.get_bloomfield_shares(con = JPOD_CONN, month = "full_sample")
for m in months:
    field_shares = pd.concat([field_shares, jpod_tests.get_bloomfield_shares(con = JPOD_CONN, month = m)])
plot_df = jpod_tests.plot_groups(df = field_shares, group="field", n_periods=len(months) +1)
jpod_tests.plot_dist(df = plot_df, group="field", outcome="share")
#plt.legend()
#plt.show()
plt.savefig("tests/img/bloomfields_dist.png")

# E) get spatial distribution of bloom_tech in full sample & snapshot sample --------------------------------
tech = "bloom"
months = ["2021-05", "2021-09", "2021-01"]
tech_shares = jpod_tests.get_spatial_tech(con = JPOD_CONN, month = "full_sample", tech = tech)
for m in months:
    tech_shares = pd.concat([tech_shares, jpod_tests.get_spatial_tech(con = JPOD_CONN, month = m, tech = tech)])
plot_df = jpod_tests.plot_groups(df = tech_shares, group="region", n_periods=len(months) +1)
jpod_tests.plot_dist(df = plot_df, group="region", outcome="share")
plt.legend()
#plt.show()
plt.savefig("tests/img/bloom_nuts_dist.png")

# F) get AI distribution of full sample & snapshot sample --------------------------------
tech = "ai"
months = ["2021-05", "2021-09", "2021-01"]
tech_shares = jpod_tests.get_tech_share(con = JPOD_CONN, month = "full_sample", tech = tech)
for m in months:
    tech_shares = pd.concat([tech_shares, jpod_tests.get_tech_share(con = JPOD_CONN, month = m, tech = tech)])
print("Distribution of job postings with connection to Acemoglu (2022) AI technology:")
print(tech_shares)
#Distribution of job postings with connection to Acemoglu (2022) AI technology:
#   tech_postings       sample         tech  total_postings     share
#0          33847  full_sample  acemoglu_ai         3211219  0.010540
#0           1460      2021-05  acemoglu_ai          175460  0.008321
#0           1099      2021-09  acemoglu_ai          165916  0.006624
#0           1186      2021-01  acemoglu_ai          153646  0.007719

# G) get spatial distribution of AI in full sample & snapshot sample--------------------------------
tech = "ai"
months = ["2021-05", "2021-09", "2021-01"]
tech_shares = jpod_tests.get_spatial_tech(con = JPOD_CONN, month = "full_sample", tech = tech)
for m in months:
    tech_shares = pd.concat([tech_shares, jpod_tests.get_spatial_tech(con = JPOD_CONN, month = m, tech = tech)])
plot_df = jpod_tests.plot_groups(df = tech_shares, group = "region", n_periods= len(months) + 1)
jpod_tests.plot_dist(df = plot_df, outcome="share")
plt.legend()
#plt.show()
plt.savefig("tests/img/ai_nuts_dist.png")