#!/bin/bash

## specify the directory ---------
jpod_src="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/jpod/"
jpod_data="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"

## create sqlite database ---------
ml load SQLite/3.35.4-GCCcore-10.3.0
cd $data_path
sqlite3 jpod.db ".read ${jpod_src}create_jpod.sqlite"
ml purge

## insert base job adds data using python ---------
ml load Python
source "${jpod_data}/jpod_venv/bin/activate"
## arg[1] = path to database, arg[2] = path to raw data, arg[3] = file format of raw data:
python "${jpod_src}data_insert.py" $jpod_data "${jpod_data}jobspickr_raw/" ".zip" 