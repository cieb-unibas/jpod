import urllib.request as urlreq
import os
import sys

sys.path.append(os.getcwd())
from jpod import config
from jpod.navigate import get_path

def get_config(config_file):
    with open(config_file, "r") as f:
        urls = f.read().split("\n")
    return urls

def save_jobspickr(url, save_at):
    request = urlreq.Request(url)
    response = urlreq.urlopen(request)    
    with open(save_at, 'wb') as f:
        f.write(response.read())
    print("file saved as %s" % save_at)

if __name__ == "__main__":
    print("----------------Ready to download postings from jobspickr----------------")
    DATA_BATCH = config.BATCH_VERSION
    destination = get_path(potential_paths=config.DAT_DIRS) + DATA_BATCH
    urls = get_config(config_file = os.path.join(destination, "jobspickr_config.txt"))
    for u in urls:
        file_name = u.split("/")[-1]
        save_jobspickr(url = u, save_at = os.path.join(destination, file_name))
    print("----------------All files from JobsPickr saved to disk.----------------")
