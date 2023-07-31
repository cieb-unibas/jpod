#!/bin/bash
#SBATCH --job-name=main_analysis_jpod
#SBATCH --cpus-per-task=8
#SBATCH --mem=12G

#SBATCH --time=01:30:00
#SBATCH --qos=6hours

#SBATCH --output=main_analysis_jpod
#SBATCH --error=data/main_analysis_jpod_errors
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

cd "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/jpod/"
source ../jpod_venv/bin/activate
python main.py