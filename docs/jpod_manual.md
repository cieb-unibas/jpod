# JOPD MANUAL
JPOD is short for 'Job Postings Database'. It hosts data on job adds the CIEB first acquired in 2022 from JobsPickr. The idea of JPOD is to have an easily updatable and managable database that allows to store further data in the future - be it from JobsPickR or other sources.

## JPOD in Brief
JPOD is set up as a SQLite Database (https://www.sqlite.org/), which is one of the most common relational database management systems (RDMS) in the world. SQLite is especially suitable for releatively 'small' databases and is easily transferrable since the entire database is stored as a single file. SQLite features a lightweight command line programm called 'sqlite3', which allows to execute SQL statements from the CL. Besides, sqlite is compatible to interact with all kinds of IDEs (e.g. DBeaver, DB Browser, Beekeper Studio) and there are several solutions to directly interact with sqlite using Python and/or R libraries.  

JPOD is stored on scicore in the CIEB's GROUP folder under the following path.

## Setting Up JPOD: Insert JobsPickR Data


## Enhancing JPOD: Inserting Regional, Industry and Country Information


## Updating JPOD: New job adds