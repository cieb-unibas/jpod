#!/bin/bash
#SBATCH --job-name=add_geodata_jpod
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G

#SBATCH --time=00:30:00
#SBATCH --qos=30min

#SBATCH --output=add_geodata_logs
#SBATCH --error=add_geodata_errors
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

## add information to JPOD using Python
jpod_path="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
ml load Python/3.9.5-GCCcore-10.3.0
source "${jpod_path}/jpod_venv/bin/activate"
python "${jpod_path}jpod/jpod/insert_geo.py" $jpod_path