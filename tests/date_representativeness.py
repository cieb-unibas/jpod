import sys
import pandas as pd
import sqlite3

# connect to the databse
DB_DIR = sys.argv[1]
DB_VERSION = sys.argv[2]
JPOD_CONN = sqlite3.connect(DB_DIR + DB_VERSION)

# get nuts_2 distribution of full sample & snapshot sample

# get nuts_3 distribution of full sample & snapshot sample

# get bloom_tech distribution of full sample & snapshot sample

# get ai distribution of full sample & snapshot sample

# get spatial distribution of bloom_tech in full sample & snapshot sample

# get spatial distribution of ai in full sample & snapshot sample
