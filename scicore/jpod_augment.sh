#!/bin/bash
#SBATCH --job-name=jpod_tests
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G

#SBATCH --time=01:00:00
#SBATCH --qos=6hours

#SBATCH --output=tests/jpod_tests_logs
#SBATCH --error=tests/jpod_tests_logs
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

## add information to JPOD using Python
jpod_path="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
jpod_version="jpod.db"
ml load Python/3.9.5-GCCcore-10.3.0
source "${jpod_path}/jpod_venv/bin/activate"
python "${jpod_path}jpod/tests/date_representativeness.py" $jpod_path $jpod_version