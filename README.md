# JPOD

Source code for creating and updating the CIEB Job Postings Database (JPOD)

## What is JPOD?
JPOD hosts job adds data the CIEB first acquired in 2022 from <a href='https://www.jobspikr.com/'>jobspickr</a>. The idea of JPOD is to have an easily updateable and manageable database that allows to add further job-ads data in the future - be it from jobspickr or other providers.

### JPOD in Brief
JPOD is set up as a SQLite Database (https://www.sqlite.org/), which is stored on the scicore cluster in the CIEB's `/GROUP/` folder under the directory `/scicore/home/weder/GROUP/Innovation/05_job_adds_data/`. The created sqlite database is stored in the file `jpod.db` (about 68.4 GB). The source code is available in the directory `jpod/`, which is also this GitHub repository (https://github.com/cieb-unibas/jpod). A virtual environment containing all the necessary dependencies is available at `jpod-venv/`. Activate this virtual environment using `source jpod-venv/bin/activate` to interact with JPOD.

Currently (i.e. August 2023), JPOD contains 9'196'097 job postings from 21 countries and 701'828 different employers (among which 3'211'219 job postings are from 2020-2022 period from Switzerland).

## Overview

JPOD currently consists of 6 tables, which ecompass information along the following lines:

table|descirption
---|---
job_postings|All information surrounding the job posting itself. This particularly refers to the actual text of the posting, as well as e.g., the posting date and whether or not it is unique or duplicated in the databse.
position_characteristics|All information about the position the job posting encompasses. This refers to postings employer but also e.g., the region of the specified location and whether or not the posting describes a remote position.
institutions|All information about the employers that are registered in the database. This table features information regarding e.g., employer websites and contact information.
acemoglu_ai|A lists of postings that have AI-specific keywords as defined by <a href='https://www.journals.uchicago.edu/doi/full/10.1086/718327'>Acemoglu et al. (2022) (Footnote 13)</a> in the postings text.  
bloom_tech|A lists of postings that have technology-specific keywords as defined by <a href='https://www.nber.org/papers/w28999'>Bloom et al. </a> in the postings text.
regio_grid|An overview table of regional entities that are present in JPOD. This selection is motivated by Eurostat's [NUTS regions](https://ec.europa.eu/eurostat/web/nuts/background/), as well as the [OECD territorial Grid Taxonomy](https://www.oecd.org/regional/regional-statistics/territorial-grid.pdf) and can be further extended.

Figure xxx shows the full schema of JPOD:

![](./figures/jpod_regio_er.png)


For the full technical docmentation see <a href='./docs/jpod_manual.md'>JPOD manual</a>

## Usage

To use the JPOD functionalities for another project, create a virtual environment, activate it, `cd` into this repository and run
`python3 -m pip install -e <PATH_TO_THIS_REPOSITORY>`

