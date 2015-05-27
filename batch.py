import os
from os import walk
import thread
import gzip
from multiprocessing import Pool, Process, Queue

PROCESS_COUNT = 8

table = "gnip.tweets_io15"
mypath = "data"

class Utils:

    @staticmethod
    def rename(file, to):

        call_rename = "mv %s %s" % (file, to)
        print call_rename
        os.system(call_rename)

        return to

    @staticmethod
    def archive(file):

        if ".archive" in file:
            return file 

        file_archive = "%s.archive" % file
        Utils.rename(file, file_archive)

        return file_archive

    @staticmethod
    def unarchive(file):

        if ".archive" not in file:
            return file 

        file2 = file[:-8]
        Utils.rename(file, file2)

        return file2

    @staticmethod
    def gzip(file):

        if ".gz" in file:
            return file 

        call_zip = "gzip %s" % (file)
        print call_zip
        os.system(call_zip)

        return "%s.gz" % file

    @staticmethod
    def gunzip(file):

        if ".gz" not in file:
            return file 

        call_unzip = "gunzip %s" % (file)
        print call_unzip
        os.system(call_unzip)

        return file[:-3]

    @staticmethod
    def cat_all(path, file):

        call_cat = "find %s -type f -exec cat {} + > " % (path, file)
        print call_cat
        os.system(call_cat)

        return file

def reset_file(file, output=None):

    # ignore archive files
    if "json.gz" in file:
        
        Utils.gunzip(file)

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
        file = Utils.gunzip(file)

    # load to bigquery
    call_batch = "bq load --source_format=NEWLINE_DELIMITED_JSON --max_bad_records=5000 %s %s" % (table, file)
    print call_batch
    os.system(call_batch)

    # pack file back up so it doesn't take up any more memory
    file_gz = Utils.gzip(file)

    # archive processed file (re-entrant processing)
    Utils.archive(file_gz)

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
    results = [pool.apply_async(reset_file, args=(f,)) for f in files]

    output = [p.get() for p in results]
    print output


