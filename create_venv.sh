#!/bin/bash
ml load Python
jpod_data="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"
virtualenv --system-site-packages "${jpod_data}/jpod_venv"
source "${jpod_data}/jpod_venv/bin/activate"
pip install -r "${jpod_data}/jpod/requirements.txt"
deactivate
