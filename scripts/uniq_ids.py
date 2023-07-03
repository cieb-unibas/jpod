import os
import sys
import csv

print("Current directory is: " + os.getcwd())
sys.path.append(os.getcwd())
import jpod

if __name__ == "__main__":

    # set parameters
    JPOD_VERSION = "jpod_test.db"
    DATA_BATCH = "jobspickr_2023_01"
    DB_DIR = os.path.join(jpod.get_path(jpod.config.DB_DIRS), JPOD_VERSION)
    AUGMENT_PATH = "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/augmentation_data/"
    if not os.path.exists(AUGMENT_PATH):
        AUGMENT_PATH = jpod.get_path(jpod.config.DAT_DIRS)

    # create uid chunks and find duplicated uniq_ids
    FILES = jpod.select_raw_files(dir = jpod.get_path(jpod.config.DAT_DIRS))
    UID_CHUNCK_DICT = {}
    DUPLICATE_IDS = []

    for file in FILES:
        # load the uniq_ids from the raw file
        uniq_ids = jpod.load_raw_data(os.path.join(jpod.get_path(jpod.config.DAT_DIRS), file))["uniq_id"]
        # add all ids to the dict and filter out duplicates
        for uid in uniq_ids:
            chunck_id = uid[:2]
            if chunck_id not in UID_CHUNCK_DICT.keys():
                UID_CHUNCK_DICT[chunck_id] = [uid]
            else:
                if uid in UID_CHUNCK_DICT[chunck_id]:
                    DUPLICATE_IDS.append(uid)
                else:
                    UID_CHUNCK_DICT[chunck_id].append(uid)
        
        # save duplicates as a text file
        if len(DUPLICATE_IDS) > 0:
            with open(AUGMENT_PATH + "duplicated_uniq_ids.csv", "w", newline="") as f:
                writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                writer.writerow(DUPLICATE_IDS)
            print("Duplicated ids saved to disk")
        else:
            print("No duplicted ids found")