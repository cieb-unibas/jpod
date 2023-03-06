#!/bin/bash

#SBATCH --job-name=launch_jpod
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G

#SBATCH --time=04:30:00
#SBATCH --qos=6hours

#SBATCH --output=cluster_logs/launch_jpod1_1
#SBATCH --error=cluster_logs/launch_jpod1_1_errors
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

## directories
jpod_path="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
cd $jpod_path

## create sqlite database
ml load SQLite/3.35.4-GCCcore-10.3.0
sqlite3 jpod.db ".read ${jpod_path}jpod/scicore/jpod_create.sqlite"
ml purge

## insert base data from JobsPickR using Python
ml load Python/3.9.5-GCCcore-10.3.0
source "${jpod_path}/jpod_venv/bin/activate"
python "${jpod_path}jpod/jpod/insert_base.py" $jpod_path