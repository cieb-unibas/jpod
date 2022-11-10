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
sqlite3 $jpod_version ".read ${jpod_test_path}jpod/scicore/jpod_create.sqlite"
ml purge

## insert base data from JobsPickR using Python
ml load Python/3.9.5-GCCcore-10.3.0
source "${jpod_test_path}/jpod_venv/bin/activate"
cd ./jpod/
python scicore/test_db.py $jpod_test_path

## add geographical information & detect postings with a connection to AI and disruptive technologies from Bloom(2021)
for f in assign_geo.py detect_ai.py detect_disruptech.py
do
  python jpod/$f $jpod_test_path $jpod_version
done