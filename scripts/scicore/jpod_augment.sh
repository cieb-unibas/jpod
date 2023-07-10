#!/bin/bash
#SBATCH --job-name=augment_jpod
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G

#SBATCH --time=06:00:00
#SBATCH --qos=6hours

#SBATCH --output=cluster_logs/clean_duplicates
#SBATCH --error=cluster_logs/clean_duplicates_errors
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

## configure
jpod_path="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
jpod_version="jpod.db"
cd $jpod_path/jpod/
source ../jpod_venv/bin/activate

## add infromation to JPOD
python ./scripts/clean_duplicates.py