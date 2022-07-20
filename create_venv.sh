#!/bin/bash
##ml load Python/3.7.2-GCCcore-8.2.0
ml load Python/3.9.5-GCCcore-10.3.0
jpod_path="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
virtualenv --system-site-packages "${jpod_path}/jpod_venv"
source "${jpod_path}/jpod_venv/bin/activate"
pip install -r "${jpod_path}/jpod/requirements.txt"
deactivate
ml purge