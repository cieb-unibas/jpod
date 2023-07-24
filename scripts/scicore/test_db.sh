#!/bin/bash

#SBATCH --job-name=jpod_test
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G

#SBATCH --time=00:30:00
#SBATCH --qos=30min

#SBATCH --output=cluster_logs/jpod_test_logs
#SBATCH --error=cluster_logs/jpod_test_errors
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

## directories & database version
##jpod_test_path="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
##jpod_version="jpod_test.db"
##cd $jpod_test_path
##if test -f $jpod_version; then
##  rm "${jpod_test_path}${jpod_version}"
##fi

## create sqlite database 'jpod_test.db' & corresponding indexes
##ml load SQLite/3.35.4-GCCcore-10.3.0
##sqlite3 $jpod_version ".read ${jpod_test_path}jpod/scripts/jpod_test_create.sqlite"
##sqlite3 $jpod_version ".read ${jpod_test_path}jpod/scripts/jpod_index.sqlite"
##ml purge

## insert data from JPOD to jpod_test.db using python
source ./jpod_venv/bin/activate
cd ./jpod/
python ./scripts/create_test_db.py