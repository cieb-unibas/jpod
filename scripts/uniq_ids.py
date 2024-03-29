import os
import sys

import pandas as pd

print("Current directory is: " + os.getcwd())
sys.path.append(os.getcwd())
import jpod

if __name__ == "__main__":

    # set parameters
    DATA_BATCH = jpod.config.BATCH_VERSION
    OUT_PATH = jpod.get_path(jpod.config.DAT_DIRS) + DATA_BATCH
    
    DATA_DIR = jpod.get_path(jpod.config.DAT_DIRS)
    BATCHES = os.listdir(DATA_DIR)
    FILES = []
    for batch in BATCHES:
        batch_path = os.path.join(DATA_DIR, batch)
        FILES += [os.path.join(batch_path, file) for file in jpod.select_raw_files(dir = batch_path)]    

    # create uid chunks and find duplicated uniq_ids
    UID_CHUNCK_DICT = {}
    DUPLICATE_IDS = []

    for i, file in enumerate(FILES[:2]):
        # load the uniq_ids from the raw file
        print(file)
        uniq_ids = jpod.load_raw_data(file)["uniq_id"]
        print(len(uniq_ids))
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
        # logger
        if i % 1 == 0:
            print("Identified duplicated uniq_id's in %d/%d raw files." % (i + 1, len(FILES)))

    # save to disk
    if len(DUPLICATE_IDS) > 0:
        DUPLICATE_IDS = pd.Series(DUPLICATE_IDS, name = "duplicated_ids")
        DUPLICATE_IDS.to_csv(os.path.join(OUT_PATH, "duplicated_ids.csv"), index = False)
        print("%d duplicated 'uniq_id's saved to disk." % len(DUPLICATE_IDS))
    else:
        print("No duplicated 'uniq_id's found")