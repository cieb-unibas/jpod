import sys
#sys.path.append("./jpod")
import navigate as nav
import pandas as pd
import sqlite3

# connect to JPOD
JPOD_CONN = nav.db_connect(db_path = DB_DIR)

# set techfield specific keywords and translate them to German, French, Italian

# loop over techfields and extract postings that contain their keywords

# insert the matched ones to a new table 'bloom_tech' with uniq_id, bloom_tech_field, bloom_tech_field_indx 

# maybe do the same for Gartner Hype Cycle, Goldfarb et al. (2022) and Acemoglu et al. (2020)