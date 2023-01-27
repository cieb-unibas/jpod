# JPOD MANUAL
JPOD is short for '**J**ob **Po**stings **D**atabase'. It hosts job adds data the CIEB first acquired in 2022 from JobsPickr. The idea of JPOD is to have an easily updateable and manageable database that allows to add further job-ads data in the future - be it from JobsPickR or other providers.

## JPOD in Brief
For now, JPOD is set up as a SQLite Database, which is one of the most common relational database management systems (RDBMS) in the world (https://www.sqlite.org/). SQLite is especially suitable for relatively 'small' databases and is easily transferable since the entire database is stored as a single file. SQLite features a lightweight command line program called 'sqlite3', which allows one to execute SQL statements from the CL. Besides, sqlite is compatible to interact with all kinds of IDEs (e.g. DBeaver, DB Browser, Beekeper Studio) and there are several libraries to directly interact with sqlite using Python and/or R.  

JPOD as well as its source code is stored on scicore in the CIEB's GROUP folder under the directory `/scicore/home/weder/GROUP/Innovation/05_job_adds_data/`. The created sqlite database is stored in the file `jpod.db` (about 17.2 GB). The source code is available in the directory `jpod/`, which is also on GitHub (https://github.com/cieb-unibas/jpod). 

Currently (i.e. January 2023), JPOD contains 3'211'219 Swiss job postings from 76'935 different institutions.

## Setting Up JPOD from scratch: 
The initial setup is performed along the lines of the 2022 JobsPickR data. JPOD builds on 26 variables from this batch of data. The variables are then distributed across 3 different JPOD tables: `job_postings`, `position_characteristics` and `institutions`. `job_postings` contains all information around the posting as such (e.g., the date it was scrapped, the posting text, the webpage it was published. `position_characteristics` features variables that describe the postion that is described by the posting (e.g., the location of the position, the employers' name etc.). Finally, the table `institutions` contains information about all employers that have postings in the database (e.g., the employer's webpage, the employer's phone number etc.). This initial architecture is defined in the file `scicore/create_jpod.sqlite`. 

### Inserting raw data to JPOD (2022 JobsPickR Data)
JPOD is set up on scicore using the slurm scirpt `jpod_launch.sh`. This scirpt implements the initial architecture of JPOD and then inserts the 2022 raw data from JobsPickr (this data can be found as zipped `.csv` files in the directory `/scicore/home/weder/GROUP/Innovation/05_job_adds_data/jobspickr_raw/`). Since the latter is performed using a Python script, we first need to install a Python virtual environment. Such a virtual environment can be setup via the bash script `scicore/create_venv.sh`. **However**, this is not necessary now since it already exists (For setting up virtual environments on your local machine, see the documentation of either [`conda`](https://docs.conda.io/projects/conda/en/latest/user-guide/getting-started.html#managing-environments) or [`virtualenv`](https://docs.conda.io/projects/conda/en/latest/user-guide/getting-started.html#managing-environments)).

Assuming you are in the `jpod/` root directory, the file `jpod_launch.sh` can then be sent to the cluster using the commend `sbatch scicore/launch_jpod.sh`. It sets up JPOD using the `scicore/create_jpod.sqlite` script and then calls the python script `insert_base.py`, which loads the 2022 JobsPickr raw data, performs several cleaning steps (e.g. lowercase selected columns, distinct rows only etc.) and inserts the cleaned data to the above.mentioned 3 base-tables of JPOD.

## JPOD Architecture: Tables and Keys
Since JPOD's architecture is very closely related to JobsPickR, its original setup depends on JobsPickR information. Most importantly, this concerns the column `uniq_id`, which is taken as is from the JobsPickR data. `uniq_id` is a identifier variable of length 32, consisting entirely of ASCII characters and digits. In JPOD, this column serves as a SQL primary key in the two tables `job_postings` and `position_characteristics`. These two tables and can thus be connected (i.e. joined) through the column `uniq_id` . Note: For potential future updates from other data providers than JobsPickR, it might therefore be necessary to create `uniq_id`'s from scratch. The third initial table is the `institutions` table, where information about companies can be accessed. The `institutions` table is connected to the other tables through the column `company_name`, which is also features in the `position_characteristics` table. A graphical overview of the base JPOD architecture is presented below.

**Figure 1: ER Diagram of JPOD's base architecture**

![](figures/jpod_base_er.png)

## Enhancing JPOD

### Assigning Postings to NUTS-Regions and OECD Territorial Levels.
A key feature of JPOD is to enable analyses on the regional level. To be compatible with commonly used regional taxonomies, job postings are assigned to Eurostat's [NUTS regions](https://ec.europa.eu/eurostat/web/nuts/background/), as well as the [OECD territorial Grid Taxonomy](https://www.oecd.org/regional/regional-statistics/territorial-grid.pdf). A new JPOD table `regio_grid` is added to JPOD via the `/jpod/add_geogrid.py` script, which loads data from the manually constructed file `data/regio_grid.csv`. This table consits of columns indicating a geographical entity's name in English, German and French, its abbrevation (if it exists) and all it's NUTS and OECD TL codes (if they exist). An example of rows from this table is given below:

name_en|name_de|name_fr|regio_abbrev|nuts_level|oecd_level|iso_2|iso_3|nuts_0|nuts_1|nuts_2|nuts_3|oecd_tl1|oecd_tl2|oecd_tl3
---|---|---|---|---|---|---|---|---|---|---
switzerland|schweiz|suisse|CH|1|1|CH|CHE|CH|CH0|||CH||
espace mittelland|espace mittelland|espace mittelland||2|2|CH|CHE|CH|CH0|CH02||CH|CH02|
basel-stadt|basel-stadt|bâle-ville|BS|3|3|CH|CHE|CH|CH0|CH03|CH031|CH|CH03|CH031

**Currently, this table only contains the territorial grids for Switzerland. It will be updated as soon as JPOD features postings from other countries as well.**.

The actual assignment of job postings to these Swiss regions is then performed via the script `jpod/insert_geo.py`. Using slurm, this script can also be sent to the cluster via the `scicore/jpod_augment.sh`. Job postings are assigned to territorial codes based on the `position_characteristics` columns `city`, `inferred_city`, `state`, and `inferred_state` (JobsPickR information). The values in these columns are matched to the `name_en`, `name_de`, `name_fr` and/or `regio_abbrev` columns from the `regio_grid` table. The script then creates two new columns, `nuts_2` and `nuts_3`, in the `position_characteristics` table that indicate the location for every job posting if a match could be found. Hence, **geographical information on NUTS-levels 2 and 3 are directly present in the `position_characteristics` table**. If other terrtorial information has to be retrieved (e.g. another aggregation level or the English, French or German name of the code), this can be retrieved by joining with the `regio_grid` table. A graphical overview is given in Figure 2.

**Notes on Refined Matching**: Some job postings state their geographical information in a special way (e.g. 'baselstadt' instead of 'basel-stadt' or 'bs'). To capture these false negatives to some extent, refined matching operations are performed for the largest non-matched Swiss entities via the `jpod/insert_geo.py`. After all of these operations 92.6\% of the 3'211'219 job postings in the baseline JPOD could be matched to NUTS 2 regions. 

**Figure 2: ER Diagram of JPOD's regional information**

![](figures/jpod_regio_er.png)


### Duplicate Cleaning
tbd

## Updating JPOD: Inserting New Job Adds
tbd
