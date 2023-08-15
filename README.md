# :desktop_computer: :floppy_disk: JPOD

Source code for creating and updating the CIEB Job Postings Database (JPOD)

## What is JPOD?
JPOD hosts job adds data the CIEB first acquired in 2022 from <a href='https://www.jobspikr.com/'>jobspickr</a>. The idea of JPOD is to have an easily updateable and manageable database that allows to add further job-ads data in the future - be it from jobspickr or other data providers.

JPOD is set up as a SQLite Database (https://www.sqlite.org/), which is stored on the scicore cluster in the CIEB's `/GROUP/` folder under the directory `/scicore/home/weder/GROUP/Innovation/05_job_adds_data/`. The created sqlite database is stored in the file `jpod.db` (about 68.4 GB). The source code is available in the directory `jpod/`, which is also this GitHub repository (https://github.com/cieb-unibas/jpod). A virtual environment containing all the necessary dependencies is available at `jpod-venv/`. Activate this virtual environment using `source jpod-venv/bin/activate` to interact with JPOD.

Currently (i.e. August 2023), **JPOD contains 9'196'097 job postings from 21 countries and 701'828 different employers** (among which 3'211'219 job postings are attributed to the 2020-2022 period from Switzerland).

## How to access JPOD?

JPOD can be easily accessed through the SQL command-line program named `sqlite3` or the respective `sqlite3` and `DBI` interfaces for Python and R, respectively. 

:exclamation: **Please see the <a href='./examples/'>examples</a> for a more comprehensive introduction and different ways to query JPOD.**

:warning: **IMPORTANT:** In order to experiment and test your queries, it is strongly recommended to establish a connection to `jpod_test.db`, which is a smaller clone of the fully specified JPOD database `jpod.db`. This test database contains only a random sample of all job postings in the full database (0.2\%) and is thus, much more lightwight. Your experimental queries can be processed much faster and without using the scicore computing nodes.

To access and query JPOD using the command line run the following:
```bash
# import the sqlite3 package and establish a connection to the database
cd /scicore/home/weder/GROUP/Innovation/05_job_adds_data/
sqlite3 jpod_test.db
sqlite> YOUR_SQLITE_QUERY
```

To access and query JPOD using Python run the following:
```python
# import the sqlite3 package and establish a connection to the database
import sqlite3
JPOD_CON = sqlite3.connect('/scicore/home/weder/GROUP/Innovation/05_job_adds_data/jpod_test.db')
JPOD_CON.execute('YOUR_SQLITE_QUERY').fetchall()
```

To access and query JPOD using R run the following:
```R
# import the RSQLite and DBI packages and establish a connection to the database
library("RSQLite")
library("DBI")
JPOD_CON <- dbConnect(RSQLite::SQLite(), '/scicore/home/weder/GROUP/Innovation/05_job_adds_data/jpod_test.db')
res <- dbSendQuery(JPOD_CON, 'YOU_SQLITE_QUERY')
dbFetch(res)
```

## JPOD Overview

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

![erp](./docs/figures/jpod_base_er.png)


For the full technical docmentation see <a href='./docs/jpod_manual.md'>JPOD manual</a>

## Repository Struture

This repository has the following structure:
```
jpod
├── data
├── docs
├── eval
├── jpod
├── scripts
```

- [data/](./data/): Contains raw data files that are used to enhance raw data inserted to JPOD and/or to evaluate the dataabse. The data ist either [raw data](./data/raw_data/) or manually created (e.g., [](./data/regio_grid.csv)) or created through scripts (e.g., [](./data/acemoglu_ai_keywords.csv)).

- [docs/](./docs/): Contains files that aim to facilitate the access to and the maintainance of JPOD. You can find an extended JPOD manual [jpod_manual](./docs/jpod_manual.md), approximate [script runtimes](./docs/runtimes.md) and [query examples](./docs/examples/) for R and Python.

- [eval/](./eval/): Contains several scritps to test the validity and representativeness of the jobspickr data.

- [jpod/](./jpod/): Contains Python modules that support configuration, maintainance, navigation and queriying of JPOD. There are 4 modules: config, datagen, navigate and queries. These can be indidually loaded using `from jpod import ...` or entirely together using `import jpod`.

- [scripts/](./scripts/): These are mainly Python scripts and some SQLite scirpts to create JPOD and enhance its inserted raw data. Furthermore there are shell/slurm scripts in [scripts/sciocre/] that are sending the Python scripts to the cluster. For more information see the [JPOD Manual](./docs/jpod_manual.md).

## Use the package

To use the JPOD functionalities for another project, create a virtual environment, activate it, `cd` into this repository and run
`python3 -m pip install -e <PATH_TO_THIS_REPOSITORY>`

