#!/bin/bash

#SBATCH --job-name=jpod_test
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G

#SBATCH --time=06:00:00
#SBATCH --qos=6hours

#SBATCH --output=jpod_test_logs
#SBATCH --error=jpod_test_errors
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

## directories
jpod_test_path="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"

## database
jpod_version="jpod_test.db"

## create sqlite database
ml load SQLite/3.35.4-GCCcore-10.3.0
cd $jpod_test_path
sqlite3 jpod_test.db ".read ${jpod_test_path}jpod/scicore/test_db.sqlite"
ml purge

## insert base data from JobsPickR using Python
ml load Python/3.9.5-GCCcore-10.3.0
source "${jpod_test_path}/jpod_venv/bin/activate"
python "${jpod_test_path}jpod/tests/test_db.py" $jpod_test_path

## add geographical information & detect postings with a connection to AI and disruptive technologies from Bloom(2021)
python "${jpod_test_path}jpod/jpod/assign_geo.py" $jpod_test_path $jpod_version
python "${jpod_test_path}jpod/jpod/detect_ai.py" $jpod_test_path $jpod_version
python "${jpod_test_path}jpod/jpod/detect_disruptech.py" $jpod_test_path $jpod_version
ml purge