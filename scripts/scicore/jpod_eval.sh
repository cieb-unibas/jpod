#!/bin/bash
#SBATCH --job-name=jpod_test_clean_duplicates
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G

#SBATCH --time=02:00:00
#SBATCH --qos=6hours

#SBATCH --output=tests/logs/after_duplcleaning_tests
#SBATCH --error=tests/logs/after_duplcleaning_tests_errors
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

## add information to JPOD using Python
jpod_path="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
jpod_version="jpod.db"
ml load Python/3.9.5-GCCcore-10.3.0
source "${jpod_path}/jpod_venv/bin/activate"
python "${jpod_path}jpod/eval/token_based_duplicates.py" $jpod_path $jpod_version