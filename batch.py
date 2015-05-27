import os
from os import walk
import thread
import gzip
from multiprocessing import Pool, Process, Queue

PROCESS_COUNT = 8

table = "gnip.tweets_io15"
mypath = "data"

def reset_file(file, output=None):

    # ignore archive files
    if ".archive" in file:
        
        file_gz = file[:-8]

        call_rename = "mv %s %s" % (file, file_gz)
        print call_rename
        os.system(call_rename)

    if output:
        output.put(file)

    return file

def process_file(file, output=None):

    # ignore archive files
    if ".archive" in file:
        print "ignoring archive: %s" % file
        return file

    # if zipped, unzip for loading
    if "json.gz" in file:

        file_gz = file
        file = file[:-3]

        # unpack file
        call_unzip = "gunzip %s" % (file_gz)
        print call_unzip
        os.system(call_unzip)

    # load to bigquery
    call_batch = "bq load --source_format=NEWLINE_DELIMITED_JSON --max_bad_records=5000 %s %s" % (table, file)
    print call_batch
    os.system(call_batch)

    # pack file back up so it doesn't take up any more memory
    file_gz = "%s.gz" % file
    call_zip = "gzip %s" % (file)
    print call_zip
    os.system(call_zip)

    # archive processed file (re-entrant processing)
    file_archive = "%s.archive" % file_gz
    call_rename = "mv %s %s" % (file_gz, file_archive)
    print call_rename
    os.system(call_rename)

    if output:
        output.put(file)

    return file

if __name__ == '__main__':

    files = []
    processes = []

    # process all files and let async handle archiving 
    for (dirpath, dirnames, filenames) in walk(mypath):
        for f in filenames:
            file = "%s/%s" % (dirpath, f)
            files.append(file)

    pool = Pool(processes=PROCESS_COUNT)
    results = [pool.apply_async(process_file, args=(f,)) for f in files]

    output = [p.get() for p in results]
    print output


