#!/bin/bash

## specify the directory
jpod_src="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/jpod/src/"
jpod_data="/scicore/home/weder/GROUP/Innovation/05_job_adds_data/"

## update jpod.db using Python
ml load Python
python "${jpod_src}update_jpod.py"