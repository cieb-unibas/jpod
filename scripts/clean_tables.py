import sys
import sqlite3
import os

print("Current directory is: " + os.getcwd())
sys.path.append(os.getcwd())
import jpod

if __name__ == "__main__":
    # parameters
    JPOD_VERSION = "jpod.db"
    DB_DIR = os.path.join(jpod.get_path(jpod.config.DB_DIRS), JPOD_VERSION)
    JPOD_CONN = sqlite3.connect(database = DB_DIR)
    DELETE_BATCH = "jobspickr_2023_01"
    
    # clean identifier columns of a certain data_batch (running order is important!)
    for table in ["position_characteristics", "job_postings"]:
        jpod.empty_table(
            table = table, 
            conn = JPOD_CONN,
            data_batch = DELETE_BATCH
            )
        print("Cleaned table %s" % table) 
        
    # empty other tables
    JPOD_CONN.execute("DELETE FROM institutions WHERE company_name NOT IN (SELECT company_name FROM position_characteristics)")
    print("Cleaned table institutions")
    JPOD_CONN.execute("DELETE FROM acemoglu_ai WHERE uniq_id NOT IN (SELECT uniq_id FROM job_postings)") 
    print("Cleaned table acemoglu_ai")
    JPOD_CONN.execute("DELETE FROM bloom_tech WHERE uniq_id NOT IN (SELECT uniq_id FROM job_postings)")
    print("Cleaned table bloom_tech")
    
    # commit and close
    JPOD_CONN.commit()
    JPOD_CONN.close()
    
    print("Database tables cleaned")