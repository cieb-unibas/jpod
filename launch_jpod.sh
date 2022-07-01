#!/bin/bash

## directories
jpod_path="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"

## create sqlite database
ml load SQLite/3.35.4-GCCcore-10.3.0
cd $jpod_path
sqlite3 jpod.db ".read ${jpod_path}jpod/create_jpod.sqlite"
ml purge

## insert base data using python
ml load Python
source "${jpod_path}/jpod_venv/bin/activate"
python "${jpod_path}jpod/jpod/insert_base.py" $jpod_path
ml purge