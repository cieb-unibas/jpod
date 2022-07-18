#!/bin/bash

#SBATCH --job-name=launch_jpod
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G

#SBATCH --time=05:00:00
#SBATCH --qos=6hours

#SBATCH --output=launch_jpod_logs
#SBATCH --error=launch_jpod_errors
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

## directories
ml purge
jpod_path="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"

## create sqlite database
ml load SQLite/3.35.4-GCCcore-10.3.0
cd $jpod_path
sqlite3 jpod.db ".read ${jpod_path}jpod/create_jpod.sqlite"
ml purge

## insert base data from JobsPickR using Python
ml load Python/3.7.4-GCCcore-8.3.0
source "${jpod_path}/jpod_venv/bin/activate"
python "${jpod_path}jpod/jpod/insert_base.py" $jpod_path
python "${jpod_path}jpod/jpod/insert_geo.py" $jpod_path
ml purge