from jpod.navigate import connect_to_jpod
from jpod import queries

def calculate_ai_shares(jpod_version : str, save_file_path: str = "./data/ai_dist_region.csv") -> None:
    """Retrieve the ai-share of different regions internationally"""
    # establishes the connection to JPOD
    jpod_con = connect_to_jpod(version=jpod_version)
    # retrieves the total number and the number of ai-related job-postings at regional-level 2 (= oecd_tl2)
    total_jobs = queries.get_total_postings_by_region(con = jpod_con, regional_level=2)
    ai_jobs = queries.get_ai_postings_by_region(con = jpod_con, regional_level=2)
    # combine and calculate ai-shares per region
    ai_jobs = ai_jobs.merge(total_jobs, how="left", on=["region_code", "region"])
    ai_jobs["ai_share"] = ai_jobs["n_ai_postings"] / ai_jobs["n_postings"]
    ai_jobs = ai_jobs.sort_values("ai_share", ascending=False).reset_index(drop = True)
    print("Geographic distribution of AI-related job postings: \n", ai_jobs)
    # save the file at specified location
    if save_file_path:
        ai_jobs.to_csv(save_file_path, index=False)
        print("Geographic distribution of AI-related job postings saved as a .csv at the following location: ", save_file_path)

if __name__ == "__main__":
    calculate_ai_shares(jpod_version = "full")
