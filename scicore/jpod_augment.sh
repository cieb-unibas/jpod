#!/bin/bash
#SBATCH --job-name=augment_jpod
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G

#SBATCH --time=06:00:00
#SBATCH --qos=6hours

#SBATCH --output=cluster_logs/jpod1_1_cleaner
#SBATCH --error=cluster_logs/jpod1_1_augment_errors
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

## add information to JPOD using Python
jpod_path="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
jpod_version="jpod.db"
cd $jpod_path

ml load Python/3.9.5-GCCcore-10.3.0
source ./jpod_venv/bin/activate

## add geographical information, detect postings with a connection to AI, disruptive technologies from Bloom(2021) and clean duplicates.
## full augmentation
##for f in add_geogrid.py assign_geo.py detect_ai.py detect_disruptech.py clean_duplicates.py
##do
##  python ./jpod/jpod/$f $jpod_path $jpod_version
##done

## specific augmentation
python ./jpod/jpod/clean_duplicates.py $jpod_path $jpod_version