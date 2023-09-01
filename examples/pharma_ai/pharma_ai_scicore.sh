#!/bin/bash
#SBATCH --job-name=pharma_ai_share
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G

#SBATCH --time=01:30:00
#SBATCH --qos=6hours

#SBATCH --output=examples/pharma_ai/pharma_ai_share
#SBATCH --error=examples/pharma_ai/pharma_ai_share_errors
#SBATCH --mail-type=END,FAIL,TIME_LIMIT
#SBATCH --mail-user=matthias.niggli@unibas.ch

## configure
## !!!  make sure that your working directory when sending this script to the cluster
## corresponds to the jpod repository. Use `cd PATH_TO_JPOD_REPOSITORY` to do that in the console 
## or uncomment and adapte the line below !!!
## cd PATH_TO_JPOD_REPOSITORY

## run the script
ml load R
Rscript ./examples/pharma_ai/pharma_ai_data.R