import os
import sqlite3
import zipfile as zf

import pandas as pd

from jpod import config
from jpod.navigate import get_path

def list_batch_files(path):
    files = os.listdir(path)
    files = [file for file in files if file.endswith(".zip")]
    return files

def load_uniq_ids_from_file(filepath, n_samples, columns):
        data = zf.ZipFile(filepath)
        file_list = data.infolist()
        assert len(file_list) == 1
        file_name = file_list[0].filename
        data = pd.read_csv(data.open(file_name), low_memory=False)[columns].dropna()
        return data.sample(n_samples, random_state = 472023)

def create_jpod_query(cities):
     cities = ", ".join(["'" + str(city).lower() + "'" for city in cities])
     query = """
     SELECT uniq_id
     FROM position_characteristics pc
     WHERE inferred_city IN (%s);
     """ % (cities)
     return query
     
JPOD_VERSION = "jpod.db"
DATA_BATCH = config.BATCH_VERSION
DATA_DIR = get_path(config.DAT_DIRS) + DATA_BATCH
OUT_FILE = "augmentation_data/not_inserted_files.csv"

if __name__ == "__main__":
    # connect to .db
    JPOD_CONN = sqlite3.connect(database = JPOD_VERSION)
    # list files
    FILES = list_batch_files(path = DATA_DIR)
    # check for which files data is not yet in the database
    not_inserted_files = []
    for i, file in enumerate(FILES):
        test_ids = load_uniq_ids_from_file(filepath = os.path.join(DATA_DIR, file), n_samples = 5, columns = ["uniq_id", "inferred_city"])
        db_ids = pd.read_sql(sql = create_jpod_query(cities = test_ids["inferred_city"]), con = JPOD_CONN)
        if not all(uid in list(db_ids["uniq_id"]) for uid in test_ids["uniq_id"]):
            not_inserted_files.append(file)
        if i % 10 == 0:
            print("Checked %d/%d raw files" % (i, len(FILES)))
    JPOD_CONN.close()
    # save the `not_inserted_files` list to disk
    print("Raw data from %d of %d files are not inserted to JPOD yet." % (len(not_inserted_files), len(FILES)))
    pd.Series(not_inserted_files, name = "files").to_csv(OUT_FILE, index = False)
    print("Saved list of not inserted files to disk at location: %s" % os.path.join(os.getcwd(), OUT_FILE))

