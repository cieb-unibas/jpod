import urllib.request as urlreq
import os

def get_config(config_file):
    with open(config_file, "r") as f:
        urls = f.read().split("\n")
    return urls

def save_jobspickr(url, save_at):
    request = urlreq.Request(url)
    response = urlreq.urlopen(request)    
    with open(save_at, 'wb') as f:
        f.write(response.read())
    print("file saved as %s" % destination)

if __name__ == "main":
    destination = "/scicore/home/weder/GROUP/Innovation/05_job_adds_data/jobspickr_raw/jobspickr2023/"
    urls = get_config(config_file = "../jobspickr_config.txt")#"/scicore/home/weder/GROUP/Innovation/05_job_adds_data/jobspickr_raw/2023/jobspickr_config.csv")
    for u in urls:
        file_name = u.split("/")[-1]
        save_jobspickr(url = u, save_at = os.path.join(destination, file_name))
    print("All files from JobsPickr saved to disk.")
