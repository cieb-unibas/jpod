from math import ceil
from jpod.navigate import get_path
from jpod.datagen import select_raw_files

#### database directories
DB_DIRS = [
    "C:/Users/nigmat01/Desktop/", 
    "C:/Users/matth/Desktop/",
    "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
    ]

#### raw postings data directories
DAT_DIRS = [
    "C:/Users/nigmat01/Desktop/", 
    "C:/Users/matth/Documents/github_repos/", 
    "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/jobspickr_raw/jobspickr2023"
    ]

#### dictionary of update batches containing raw files
def file_per_update_round(max_postings_per_update_round = 1000000, postings_per_file = 100000):
    update_files = select_raw_files(dir = get_path(DAT_DIRS))
    n_files = len(update_files)
    rounds_per_update = ceil(n_files * postings_per_file / max_postings_per_update_round)
    files_per_update_round = n_files // rounds_per_update
    files_update_round_dict = {batch_idx: update_files[i:i+files_per_update_round] for i in range(0, n_files, files_per_update_round) for batch_idx in range(rounds_per_update)}
    return files_update_round_dict

UPDATE_BATCH_ROUNDS = file_per_update_round()