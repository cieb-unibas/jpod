import sys
import sqlite3
import os

print("Current directory is: " + os.getcwd())
sys.path.append(os.getcwd())
import jpod

if __name__ == "__main__":
    conn = sqlite3.connect(database = "../jpod.db")
    
    # clean identifier columns (running order is important!)
    for table in ["position_characteristics", "job_postings"]:
        jpod.empty_table(
            table = table, 
            conn = conn,
            data_batch = "jobspickr_2023_01"
            )
        print("Cleaned table %s" % table) 
        
    # empty other tables
    conn.execute("DELETE FROM institutions WHERE company_name NOT IN (SELECT company_name FROM position_characteristics)")
    print("Cleaned table institutions")
    conn.execute("DELETE FROM acemoglu_ai WHERE uniq_id NOT IN (SELECT uniq_id FROM job_postings)") 
    print("Cleaned table acemoglu_ai")
    conn.execute("DELETE FROM bloom_tech WHERE uniq_id NOT IN (SELECT uniq_id FROM job_postings)")
    print("Cleaned table bloom_tech")
    
    # commit and close
    conn.commit()
    conn.close()
    
    print("Database tables cleaned")