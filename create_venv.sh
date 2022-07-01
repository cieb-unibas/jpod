#!/bin/bash
ml load Python
jpod_path="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
virtualenv --system-site-packages "${jpod_path}/jpod_venv"
source "${jpod_path}/jpod_venv/bin/activate"
pip install -r "${jpod_path}/jpod/requirements.txt"
deactivate
ml purge