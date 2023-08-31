#!/bin/bash
#SBATCH --job-name=pharma_ai_share
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G

#SBATCH --time=00:30:00
#SBATCH --qos=30min

#SBATCH --output=jpod/examples/pharma_ai/pharma_ai_share
#SBATCH --error=jpod/examples/pharma_ai/pharma_ai_share
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

## configure
cd /scicore/home/weder/GROUP/Innovation/05_job_adds_data/jpod/

## run the script
Rscript ./examples/pharma_ai/pharma_ai.R