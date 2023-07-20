#!/bin/bash

#SBATCH --job-name=jpod_test
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G

#SBATCH --time=01:00:00
#SBATCH --qos=6hours

#SBATCH --output=jpod_test_logs
#SBATCH --error=jpod_test_errors
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

## directories & database version
jpod_test_path="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
jpod_version="jpod_test.db"
cd $jpod_test_path
if test -f $jpod_version; then
  rm "${jpod_test_path}${jpod_version}"
fi

## create sqlite database
ml load SQLite/3.35.4-GCCcore-10.3.0
sqlite3 $jpod_version ".read ${jpod_test_path}jpod/scripts/jpod_test_create.sqlite"
ml purge

## insert base data from JobsPickR using Python
source "${jpod_test_path}/jpod_venv/bin/activate"
cd ./jpod/
python scripts/test_db.py

## add indexes
cd $jpod_test_path
ml load SQLite/3.35.4-GCCcore-10.3.0
sqlite3 $jpod_version ".read ${jpod_test_path}jpod/scripts/jpod_index.sqlite"
ml purge
