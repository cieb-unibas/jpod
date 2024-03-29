#!/bin/bash
#SBATCH --job-name=update_jpod
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G

#SBATCH --time=06:00:00
#SBATCH --qos=6hours

#SBATCH --output=cluster_logs/jpod_update
#SBATCH --error=cluster_logs/jpod_update_errors
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

## configurate paths and virual environments
cd /scicore/home/weder/GROUP/Innovation/05_job_adds_data/jpod/
source ../jpod_venv/bin/activate

## check and clean duplicated uniq_id's in the raw data of jobspickr
python ./scripts/uniq_ids.py

## check which files of raw data have to be considered
python ./scripts/check_updated_files.py

## update the regio_grid table if necessary.
python ./scripts/add_geogrid.py

## insert raw data from these files into jpod
python ./scripts/jpod_update.py