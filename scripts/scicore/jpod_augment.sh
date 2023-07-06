#!/bin/bash
#SBATCH --job-name=augment_jpod
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G

#SBATCH --time=06:00:00
#SBATCH --qos=6hours

#SBATCH --output=cluster_logs/augment_ai
#SBATCH --error=cluster_logs/augment_ai_errors
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

## add information to JPOD using Python
jpod_path="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
jpod_version="jpod.db"
cd $jpod_path/jpod/
source ../jpod_venv/bin/activate

## add geographical information, detect postings with a connection to AI, disruptive technologies from Bloom(2021) and clean duplicates.
## full augmentation
## for f in detect_ai.py detect_disruptech.py clean_duplicates.py
## do
##  python ./scripts/$f
## done

## specific augmentation
python ./scripts/detect_ai.py