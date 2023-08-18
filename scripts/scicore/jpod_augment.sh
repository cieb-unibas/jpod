#!/bin/bash
#SBATCH --job-name=augment_jpod
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G

#SBATCH --time=06:00:00
#SBATCH --qos=6hours

#SBATCH --output=cluster_logs/clean_duplicates
#SBATCH --error=cluster_logs/clean_duplicates_errors
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

## configure
cd /scicore/home/weder/GROUP/Innovation/05_job_adds_data/jpod/
source ../jpod_venv/bin/activate

## enhance JPOD
python ./scripts/clean_duplicates.py
##python ./scripts/detect_disruptech.py
##python ./scripts/detect_ai.py