#!/bin/bash

## create sqlite database
ml load SQLite/3.35.4-GCCcore-10.3.0
cd /scicore/home/weder/GROUP/Innovation/05_job_adds_data/
sqlite3 jpod.db ".read /scicore/home/weder/GROUP/Innovation/05_job_adds_data/jpod/src/create_jpod.sqlite"
ml purge

## insert job adds data using Python API
ml load Python
python "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/jpod/src/insert_postings.py" 