#!/bin/bash
#SBATCH --job-name=check_files_in_db
#SBATCH --cpus-per-task=8
#SBATCH --mem=12G

#SBATCH --time=00:30:00
#SBATCH --qos=30min

#SBATCH --output=cluster_logs/check_files_in_db
#SBATCH --error=cluster_logs/check_files_in_db_errors
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

cd "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
source jpod_venv/bin/activate
python check_updated_files.py